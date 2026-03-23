package auth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func GetTokenExpiry(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, fmt.Errorf("invalid JWT")
	}

	// Decode the payload (part 2), adding padding if needed
	payload := parts[1]
	if m := len(payload) % 4; m != 0 {
		payload += strings.Repeat("=", 4-m)
	}

	decoded, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return time.Time{}, err
	}

	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(decoded, &claims); err != nil {
		return time.Time{}, err
	}

	if claims.Exp == 0 {
		return time.Time{}, fmt.Errorf("no exp claim")
	}

	return time.Unix(claims.Exp, 0), nil
}

func IsTokenExpired(token string) bool {
	expiry, err := GetTokenExpiry(token)
	if err != nil {
		return true // invalid tokens are treated as expired
	}
	return time.Now().After(expiry)
}

type User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type ValidationResult struct {
	User  User
	Token string
}

func ValidateToken(authUrl, token string) (*ValidationResult, error) {
	url := fmt.Sprintf("%s/api/collections/users/auth-refresh", authUrl)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not reach auth server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("token is invalid or expired")
	}

	var result struct {
		Record User   `json:"record"`
		Token  string `json:"token"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse auth-refresh response: %w", err)
	}

	if result.Token == "" {
		return nil, fmt.Errorf("auth-refresh response did not include a rotated token")
	}

	return &ValidationResult{
		User:  result.Record,
		Token: result.Token,
	}, nil
}
