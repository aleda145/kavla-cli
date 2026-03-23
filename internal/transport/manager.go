package transport

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/aleda145/kavla-cli/internal/auth"
	"github.com/aleda145/kavla-cli/internal/session"
	"nhooyr.io/websocket"
)

type DisconnectReason string

const (
	DisconnectReasonLocalShutdown DisconnectReason = "local_shutdown"
	DisconnectReasonRemoteClose   DisconnectReason = "remote_close"
	DisconnectReasonReadError     DisconnectReason = "read_error"
)

type DisconnectEvent struct {
	Reason DisconnectReason
	Err    error
}

type Manager struct {
	client       *Client
	session      *session.Session
	ctx          context.Context
	cancel       context.CancelFunc
	Done         chan DisconnectEvent
	stopOnce     sync.Once
	doneOnce     sync.Once
	mu           sync.Mutex
	stopEvent    DisconnectEvent
	hasStopEvent bool
	prepared     bool
	verbose      bool
}

func NewManager(workerUrl, roomId, token string, sources map[string]auth.SourceConfig, verbose bool) *Manager {
	return &Manager{
		client:  NewClient(workerUrl, roomId, token),
		session: session.New(sources),
		Done:    make(chan DisconnectEvent, 1),
		verbose: verbose,
	}
}

func (m *Manager) Prepare() error {
	m.session.SetLogger(m.Log)
	m.session.SetVerboseLogger(m.Verbose)
	if m.prepared {
		return nil
	}
	if err := m.session.Start(); err != nil {
		return fmt.Errorf("failed to start session: %w", err)
	}
	m.prepared = true
	return nil
}

func (m *Manager) Start() error {
	if err := m.Prepare(); err != nil {
		return err
	}

	m.ctx, m.cancel = context.WithCancel(context.Background())
	if err := m.client.Connect(m.ctx); err != nil {
		return err
	}

	m.Log("Connected. Waiting for queries... (Ctrl+C to disconnect)\n")

	if err := m.client.SendJSON(map[string]interface{}{
		"type":    "cli_sources",
		"payload": m.session.SourceList(),
	}); err != nil {
		m.session.Close()
		_ = m.client.Close()
		return fmt.Errorf("failed to send source list: %w", err)
	}

	go m.readLoop(m.ctx)

	return nil
}

func (m *Manager) readLoop(ctx context.Context) {
	for {
		msg, err := m.client.Read(ctx)
		if err != nil {
			event := m.classifyDisconnect(err)
			switch event.Reason {
			case DisconnectReasonLocalShutdown:
				log.Printf("Disconnected locally.")
			case DisconnectReasonRemoteClose:
				log.Printf("Remote closed connection: %v", err)
			case DisconnectReasonReadError:
				log.Printf("Read error: %v", err)
			}
			m.Stop(event.Reason, event.Err)
			return
		}

		if m.client.ResolvePending(msg) {
			continue
		}

		msgType, _ := msg["type"].(string)
		switch msgType {
		case "get_source_tables":
			request, err := ParseGetSourceTablesMessage(msg)
			if err != nil {
				m.Log("Invalid table list request: %v\n", err)
				continue
			}
			m.Log("Table list requested for source: %s\n", request.SourceName)
			go m.session.HandleGetTables(m.client, request.RequestID, request.SourceName)
		case "get_source_schema":
			request, err := ParseGetSourceSchemaMessage(msg)
			if err != nil {
				m.Log("Invalid source schema request: %v\n", err)
				continue
			}
			m.Log("Schema requested for table: %s\n", request.TableRef)
			go m.session.HandleGetSourceSchema(m.client, request.RequestID, request.TableRef)
		case "get_source_stats":
			request, err := ParseGetSourceStatsMessage(msg)
			if err != nil {
				m.Log("Invalid source stats request: %v\n", err)
				continue
			}
			m.Log("Stats requested for table: %s\n", request.TableRef)
			go m.session.HandleGetSourceStats(m.client, request.RequestID, request.TableRef)
		case "query_request":
			req, err := ParseQueryRequestMessage(msg)
			if err != nil {
				m.Log("Invalid query request payload: %v\n", err)
				continue
			}
			go m.session.HandleQuery(m.client, req)
		case "cancel_query":
			request, err := ParseCancelQueryMessage(msg)
			if err != nil {
				m.Log("Invalid cancel query payload: %v\n", err)
				continue
			}
			queryLabel := request.QueryName
			if queryLabel == "" {
				queryLabel = request.ShapeID
			}
			m.Log("Received cancel request for %s\n", queryLabel)
			if cancelledName, ok := m.session.CancelQuery(request.ShapeID); ok {
				if cancelledName == "" {
					cancelledName = queryLabel
				}
				m.Log("Cancelled %s\n", cancelledName)
			} else {
				m.Log("No in-flight query found for %s\n", queryLabel)
			}
		}
	}
}

func (m *Manager) Stop(reason DisconnectReason, err error) {
	m.stopOnce.Do(func() {
		event := DisconnectEvent{
			Reason: reason,
			Err:    err,
		}

		m.mu.Lock()
		m.stopEvent = event
		m.hasStopEvent = true
		m.mu.Unlock()

		if m.cancel != nil {
			m.cancel()
		}
		if m.session != nil {
			_ = m.session.Close()
			m.prepared = false
		}
		if m.client != nil {
			_ = m.client.Close()
		}

		m.signalDone(event)
	})
}

func (m *Manager) Log(format string, args ...interface{}) {
	line := fmt.Sprintf(format, args...)
	fmt.Print(line)
	if clean := trimTrailingNewlines(line); clean != "" {
		m.client.SendOutput(clean)
	}
}

func (m *Manager) Verbose(format string, args ...interface{}) {
	if !m.verbose {
		return
	}
	m.Log(format, args...)
}

func trimTrailingNewlines(line string) string {
	for len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	return line
}

func (m *Manager) signalDone(event DisconnectEvent) {
	m.doneOnce.Do(func() {
		m.Done <- event
	})
}

func (m *Manager) classifyDisconnect(err error) DisconnectEvent {
	if err == nil {
		return DisconnectEvent{Reason: DisconnectReasonLocalShutdown}
	}

	m.mu.Lock()
	event := m.stopEvent
	hasStopEvent := m.hasStopEvent
	m.mu.Unlock()

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if hasStopEvent {
			return event
		}
		return DisconnectEvent{Reason: DisconnectReasonLocalShutdown, Err: err}
	}

	switch websocket.CloseStatus(err) {
	case websocket.StatusNormalClosure, websocket.StatusGoingAway:
		return DisconnectEvent{Reason: DisconnectReasonRemoteClose, Err: err}
	case -1:
		return DisconnectEvent{Reason: DisconnectReasonReadError, Err: err}
	default:
		return DisconnectEvent{Reason: DisconnectReasonRemoteClose, Err: err}
	}
}
