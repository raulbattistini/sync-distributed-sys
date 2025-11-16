package main

import (
	"go-rpc/configs"
	"go-rpc/internal/util"
)

func main() {
	serverCfg, _ := configs.LoadConfig("server-config.yaml", "server")

	util.Log().Info("Configs for server loaded: %v", serverCfg)
}
