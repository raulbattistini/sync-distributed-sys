package server

import (
	"go-rpc/configs"
	"go-rpc/internal/coordinator"
	"go-rpc/internal/server"
	"go-rpc/internal/util"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var NetServerAddress int
var NetServerHost string

func main() {
	serverCfg, _ := configs.LoadConfig("server-config.yaml", "server")
	parsedCfgServer := serverCfg.(configs.ServerConfig)
	NetServerAddress = parsedCfgServer.Address
	NetServerHost = parsedCfgServer.Host

	util.Log().Info("Configs for server loaded: %v", serverCfg)

	coordTimeout := time.Duration(parsedCfgServer.SessionTimeoutSeconds) * time.Second

	// TODO: fit context aware coordinator
	coord := coordinator.NewCoordinator(coordTimeout)
	defer coord.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		util.Log().Info("Shutting down RPC Server")
		coord.Stop()
		os.Exit(0)
	}()

	util.Log().Info("Starting lock coordinator server...")
	if err := server.InitRPCServer(parsedCfgServer.Address, parsedCfgServer.Host, coord); err != nil {
		util.Log().Fatal("Failed to start RPC server: %v", err)
	}
}
