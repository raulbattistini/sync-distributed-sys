package configs

import (
	"fmt"
	"go-rpc/internal/util"
	"os"

	yaml "gopkg.in/yaml.v3"
)

type Side string

const (
	Server Side = "server"
	Client Side = "client"
)

const DefHeartbeatSeconds = 10

type confs interface {
	ClientConfig | ServerConfig
}

func LoadYAML[T confs](path string, conf Side, target *T) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	util.Log().Info("Loading config for %v RPC...", conf)
	return yaml.Unmarshal(data, &target)
}

func LoadConfig(path string, side Side) (any, error) {
	switch side {

	case Server:
		cfg := DefServerConf()
		if err := LoadYAML(path, "server", &cfg); err != nil {
			return nil, err
		}
		return cfg, nil

	case Client:
		cfg := DefClientConf()
		if err := LoadYAML(path, "client", &cfg); err != nil {
			return nil, err
		}
		return cfg, nil

	default:
		util.Log().Error("Unknown type: %v", side)
		return nil, fmt.Errorf("unknown side: %s", side)
	}
}
