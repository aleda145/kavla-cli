package auth

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type SourceConfig struct {
	Type       string `yaml:"type"`
	Connection string `yaml:"connection"`
}

type Config struct {
	Token   string                  `yaml:"token"`
	ApiUrl  string                  `yaml:"api_url,omitempty"`
	AuthUrl string                  `yaml:"auth_url,omitempty"`
	AppUrl  string                  `yaml:"app_url,omitempty"`
	Sources map[string]SourceConfig `yaml:"sources,omitempty"`
}

func GetConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".kavla", "config.yaml"), nil
}

func SaveConfig(config *Config) error {
	path, err := GetConfigPath()
	if err != nil {
		return err
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}

	data, err := yaml.Marshal(config)
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0600)
}

func LoadConfigAllowMissing() (*Config, error) {
	config, err := LoadConfig()
	if err == nil {
		return config, nil
	}
	if os.IsNotExist(err) {
		return &Config{}, nil
	}
	return nil, err
}

func IsConfigMissing(err error) bool {
	return os.IsNotExist(err)
}

func SaveToken(token string) error {
	config, err := LoadConfigAllowMissing()
	if err != nil {
		return err
	}
	config.Token = token
	return SaveConfig(config)
}

func LoadConfig() (*Config, error) {
	path, err := GetConfigPath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}
