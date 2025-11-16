package main

import (
	"go-rpc/configs"
	"go-rpc/internal/util"
)

func main() {
	clientCfg, _ := configs.LoadConfig("client-config.yaml", "client")

	util.Log().Info("Configs for client loaded: %v", clientCfg)
}
