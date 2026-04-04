package cmd

import (
	"errors"
	"reflect"
	"testing"
)

type fakeInitPrompter struct {
	answers []bool
	err     error
	labels  []string
}

func (f *fakeInitPrompter) Confirm(label string) (bool, error) {
	f.labels = append(f.labels, label)
	if f.err != nil {
		return false, f.err
	}
	if len(f.answers) == 0 {
		return false, errPromptCancelled
	}
	answer := f.answers[0]
	f.answers = f.answers[1:]
	return answer, nil
}

func TestInitRunsStepsInOrder(t *testing.T) {
	resetSourceFlags()
	defer resetSourceFlags()

	originalAddSource := runInitAddSourceStep
	originalLogin := runInitLoginStep
	originalCanConnect := runInitCanConnect
	originalConnect := runInitConnectStep
	defer func() {
		runInitAddSourceStep = originalAddSource
		runInitLoginStep = originalLogin
		runInitCanConnect = originalCanConnect
		runInitConnectStep = originalConnect
	}()

	var steps []string
	runInitAddSourceStep = func(interactive bool) error {
		steps = append(steps, "add-source")
		return nil
	}
	runInitLoginStep = func() error {
		steps = append(steps, "login")
		return nil
	}
	runInitCanConnect = func() (bool, error) {
		return true, nil
	}
	runInitConnectStep = func() error {
		steps = append(steps, "connect")
		return nil
	}

	if err := runInitWithPrompter(&fakeInitPrompter{answers: []bool{true, true, true}}, true); err != nil {
		t.Fatalf("runInitWithPrompter returned error: %v", err)
	}

	expected := []string{"add-source", "login", "connect"}
	if !reflect.DeepEqual(steps, expected) {
		t.Fatalf("expected steps %v, got %v", expected, steps)
	}
}

func TestInitSkipsAddSourceWhenDeclined(t *testing.T) {
	resetSourceFlags()
	defer resetSourceFlags()

	originalAddSource := runInitAddSourceStep
	originalLogin := runInitLoginStep
	originalCanConnect := runInitCanConnect
	originalConnect := runInitConnectStep
	defer func() {
		runInitAddSourceStep = originalAddSource
		runInitLoginStep = originalLogin
		runInitCanConnect = originalCanConnect
		runInitConnectStep = originalConnect
	}()

	var steps []string
	runInitAddSourceStep = func(interactive bool) error {
		steps = append(steps, "add-source")
		return nil
	}
	runInitLoginStep = func() error {
		steps = append(steps, "login")
		return nil
	}
	runInitCanConnect = func() (bool, error) {
		return true, nil
	}
	runInitConnectStep = func() error {
		steps = append(steps, "connect")
		return nil
	}

	if err := runInitWithPrompter(&fakeInitPrompter{answers: []bool{false, true, true}}, true); err != nil {
		t.Fatalf("runInitWithPrompter returned error: %v", err)
	}

	expected := []string{"login", "connect"}
	if !reflect.DeepEqual(steps, expected) {
		t.Fatalf("expected steps %v, got %v", expected, steps)
	}
}

func TestInitStopsWhenLoginDeclined(t *testing.T) {
	resetSourceFlags()
	defer resetSourceFlags()

	originalAddSource := runInitAddSourceStep
	originalLogin := runInitLoginStep
	originalCanConnect := runInitCanConnect
	originalConnect := runInitConnectStep
	defer func() {
		runInitAddSourceStep = originalAddSource
		runInitLoginStep = originalLogin
		runInitCanConnect = originalCanConnect
		runInitConnectStep = originalConnect
	}()

	var steps []string
	prompter := &fakeInitPrompter{answers: []bool{false, false}}
	runInitAddSourceStep = func(interactive bool) error {
		steps = append(steps, "add-source")
		return nil
	}
	runInitLoginStep = func() error {
		steps = append(steps, "login")
		return nil
	}
	runInitCanConnect = func() (bool, error) {
		return false, nil
	}
	runInitConnectStep = func() error {
		steps = append(steps, "connect")
		return nil
	}

	if err := runInitWithPrompter(prompter, true); err != nil {
		t.Fatalf("runInitWithPrompter returned error: %v", err)
	}

	var expected []string
	if !reflect.DeepEqual(steps, expected) {
		t.Fatalf("expected steps %v, got %v", expected, steps)
	}
	if !reflect.DeepEqual(prompter.labels, []string{"Set up a data source?", "Log in?"}) {
		t.Fatalf("unexpected prompt labels: %v", prompter.labels)
	}
}

func TestInitStopsAfterAddSourceError(t *testing.T) {
	resetSourceFlags()
	defer resetSourceFlags()

	originalAddSource := runInitAddSourceStep
	originalLogin := runInitLoginStep
	originalCanConnect := runInitCanConnect
	originalConnect := runInitConnectStep
	defer func() {
		runInitAddSourceStep = originalAddSource
		runInitLoginStep = originalLogin
		runInitCanConnect = originalCanConnect
		runInitConnectStep = originalConnect
	}()

	var steps []string
	runInitAddSourceStep = func(interactive bool) error {
		steps = append(steps, "add-source")
		return errors.New("boom")
	}
	runInitLoginStep = func() error {
		steps = append(steps, "login")
		return nil
	}
	runInitConnectStep = func() error {
		steps = append(steps, "connect")
		return nil
	}
	runInitCanConnect = func() (bool, error) {
		return false, nil
	}

	err := runInitWithPrompter(&fakeInitPrompter{answers: []bool{true}}, true)
	if err == nil {
		t.Fatal("expected add-source error")
	}

	expected := []string{"add-source"}
	if !reflect.DeepEqual(steps, expected) {
		t.Fatalf("expected steps %v, got %v", expected, steps)
	}
}

func TestInitContinuesWhenSourceSetupIsCancelled(t *testing.T) {
	resetSourceFlags()
	defer resetSourceFlags()

	originalAddSource := runInitAddSourceStep
	originalLogin := runInitLoginStep
	originalCanConnect := runInitCanConnect
	originalConnect := runInitConnectStep
	defer func() {
		runInitAddSourceStep = originalAddSource
		runInitLoginStep = originalLogin
		runInitCanConnect = originalCanConnect
		runInitConnectStep = originalConnect
	}()

	var steps []string
	runInitAddSourceStep = func(interactive bool) error {
		steps = append(steps, "add-source")
		return errors.New("source setup cancelled")
	}
	runInitLoginStep = func() error {
		steps = append(steps, "login")
		return nil
	}
	runInitCanConnect = func() (bool, error) {
		return true, nil
	}
	runInitConnectStep = func() error {
		steps = append(steps, "connect")
		return nil
	}

	if err := runInitWithPrompter(&fakeInitPrompter{answers: []bool{true, true, true}}, true); err != nil {
		t.Fatalf("runInitWithPrompter returned error: %v", err)
	}

	expected := []string{"add-source", "login", "connect"}
	if !reflect.DeepEqual(steps, expected) {
		t.Fatalf("expected steps %v, got %v", expected, steps)
	}
}

func TestInitStopsAfterLoginError(t *testing.T) {
	resetSourceFlags()
	defer resetSourceFlags()

	originalAddSource := runInitAddSourceStep
	originalLogin := runInitLoginStep
	originalCanConnect := runInitCanConnect
	originalConnect := runInitConnectStep
	defer func() {
		runInitAddSourceStep = originalAddSource
		runInitLoginStep = originalLogin
		runInitCanConnect = originalCanConnect
		runInitConnectStep = originalConnect
	}()

	var steps []string
	runInitAddSourceStep = func(interactive bool) error {
		steps = append(steps, "add-source")
		return nil
	}
	runInitLoginStep = func() error {
		steps = append(steps, "login")
		return errors.New("boom")
	}
	runInitCanConnect = func() (bool, error) {
		return false, nil
	}
	runInitConnectStep = func() error {
		steps = append(steps, "connect")
		return nil
	}

	err := runInitWithPrompter(&fakeInitPrompter{answers: []bool{true, true}}, true)
	if err == nil {
		t.Fatal("expected login error")
	}

	expected := []string{"add-source", "login"}
	if !reflect.DeepEqual(steps, expected) {
		t.Fatalf("expected steps %v, got %v", expected, steps)
	}
}

func TestInitContinuesWhenLoginIsCancelled(t *testing.T) {
	resetSourceFlags()
	defer resetSourceFlags()

	originalAddSource := runInitAddSourceStep
	originalLogin := runInitLoginStep
	originalCanConnect := runInitCanConnect
	originalConnect := runInitConnectStep
	defer func() {
		runInitAddSourceStep = originalAddSource
		runInitLoginStep = originalLogin
		runInitCanConnect = originalCanConnect
		runInitConnectStep = originalConnect
	}()

	var steps []string
	runInitAddSourceStep = func(interactive bool) error {
		steps = append(steps, "add-source")
		return nil
	}
	runInitLoginStep = func() error {
		steps = append(steps, "login")
		return errors.New("login cancelled")
	}
	runInitCanConnect = func() (bool, error) {
		return false, nil
	}
	runInitConnectStep = func() error {
		steps = append(steps, "connect")
		return nil
	}

	if err := runInitWithPrompter(&fakeInitPrompter{answers: []bool{true, true}}, true); err != nil {
		t.Fatalf("runInitWithPrompter returned error: %v", err)
	}

	expected := []string{"add-source", "login"}
	if !reflect.DeepEqual(steps, expected) {
		t.Fatalf("expected steps %v, got %v", expected, steps)
	}
}

func TestInitStopsWhenConnectDeclined(t *testing.T) {
	resetSourceFlags()
	defer resetSourceFlags()

	originalAddSource := runInitAddSourceStep
	originalLogin := runInitLoginStep
	originalCanConnect := runInitCanConnect
	originalConnect := runInitConnectStep
	defer func() {
		runInitAddSourceStep = originalAddSource
		runInitLoginStep = originalLogin
		runInitCanConnect = originalCanConnect
		runInitConnectStep = originalConnect
	}()

	var steps []string
	runInitAddSourceStep = func(interactive bool) error {
		steps = append(steps, "add-source")
		return nil
	}
	runInitLoginStep = func() error {
		steps = append(steps, "login")
		return nil
	}
	runInitCanConnect = func() (bool, error) {
		return true, nil
	}
	runInitConnectStep = func() error {
		steps = append(steps, "connect")
		return nil
	}

	if err := runInitWithPrompter(&fakeInitPrompter{answers: []bool{true, true, false}}, true); err != nil {
		t.Fatalf("runInitWithPrompter returned error: %v", err)
	}

	expected := []string{"add-source", "login"}
	if !reflect.DeepEqual(steps, expected) {
		t.Fatalf("expected steps %v, got %v", expected, steps)
	}
}

func TestInitContinuesWhenConnectIsCancelled(t *testing.T) {
	resetSourceFlags()
	defer resetSourceFlags()

	originalAddSource := runInitAddSourceStep
	originalLogin := runInitLoginStep
	originalCanConnect := runInitCanConnect
	originalConnect := runInitConnectStep
	defer func() {
		runInitAddSourceStep = originalAddSource
		runInitLoginStep = originalLogin
		runInitCanConnect = originalCanConnect
		runInitConnectStep = originalConnect
	}()

	var steps []string
	runInitAddSourceStep = func(interactive bool) error {
		steps = append(steps, "add-source")
		return nil
	}
	runInitLoginStep = func() error {
		steps = append(steps, "login")
		return nil
	}
	runInitCanConnect = func() (bool, error) {
		return true, nil
	}
	runInitConnectStep = func() error {
		steps = append(steps, "connect")
		return errors.New("Cancelled.")
	}

	if err := runInitWithPrompter(&fakeInitPrompter{answers: []bool{true, true, true}}, true); err != nil {
		t.Fatalf("runInitWithPrompter returned error: %v", err)
	}

	expected := []string{"add-source", "login", "connect"}
	if !reflect.DeepEqual(steps, expected) {
		t.Fatalf("expected steps %v, got %v", expected, steps)
	}
}

func TestInitRequiresInteractiveTerminal(t *testing.T) {
	err := runInitWithPrompter(&fakeInitPrompter{}, false)
	if err == nil {
		t.Fatal("expected non-interactive init to fail")
	}
}

func TestParseInitConfirmByte(t *testing.T) {
	cases := map[byte]initConfirmAnswer{
		'\r': initConfirmYes,
		'\n': initConfirmYes,
		'y':  initConfirmYes,
		'Y':  initConfirmYes,
		'n':  initConfirmNo,
		'N':  initConfirmNo,
		3:    initConfirmCancelled,
		'x':  initConfirmInvalid,
	}

	for input, expected := range cases {
		if got := parseInitConfirmByte(input); got != expected {
			t.Fatalf("input %q: expected %v, got %v", input, expected, got)
		}
	}
}
