package main

import (
	"fmt"
	"go-rpc/configs"
	"go-rpc/internal/coordinator"
	"go-rpc/internal/util"
	"net/rpc"
	"time"

	"go.bryk.io/pkg/ulid"
)

type Client struct {
	rpcClient *rpc.Client
	sessionID ulid.ULID
	clientID  string
}

var parsedCfgClient configs.ClientConfig

func NewRPCClient(address int, host string, clientID string) (*Client, error) {
	netClientAddress := fmt.Sprintf("%s:%d", host, address)
	rpcClient, err := rpc.Dial("tcp", netClientAddress)
	if err != nil {
		return nil, err
	}

	client := &Client{
		rpcClient: rpcClient,
		clientID:  clientID,
	}

	var resp coordinator.RegisterSessionResponse
	err = rpcClient.Call("CoordinatorService.RegisterSession", coordinator.RegisterSessionRequest{ClientID: clientID}, &resp)
	if err != nil {
		return nil, err
	}
	client.sessionID = resp.SessionID

	util.Log().Info("Client %s registered new session with ID: %s", clientID, client.sessionID.String())

	go client.heartbeatLoop()

	return client, nil
}

func (c *Client) heartbeatLoop() {
	heartbeatPollSec := parsedCfgClient.HeartbeatPollingSeconds
	ticker := time.NewTicker(time.Duration(heartbeatPollSec) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		var resp coordinator.HeartbeatResponse
		err := c.rpcClient.Call("CoordinatorService.Heartbeat", coordinator.HeartbeatRequest{SessionID: c.sessionID}, &resp)
		if err != nil {
			util.Log().Error("Heartbeat failed for session %s: %v", c.sessionID.String(), err)
			continue
		} else if !resp.Resp.Success {
			util.Log().Warn("Heartbeat not successful for session %s", c.sessionID.String())
			continue
		}
	}
}

// there will be waitTime for the futex thing
func (c *Client) AcquireLock(lockName string, timeout time.Duration) (*coordinator.AcquireLockResponse, error) {
	var resp coordinator.AcquireLockResponse
	req := coordinator.AcquireLockRequest{
		LockName:  lockName,
		SessionID: c.sessionID,
		Timeout:   timeout,
		WaitTime:  0,
	}
	err := c.rpcClient.Call("CoordinatorService.AcquireLock", req, &resp)
	return &resp, err
}

func (c *Client) ReleaseLock(lockName string, token string) (*coordinator.ReleaseLockResponse, error) {
	var resp coordinator.ReleaseLockResponse
	req := coordinator.ReleaseLockRequest{
		LockName:  lockName,
		Token:     token,
		SessionID: c.sessionID,
	}
	err := c.rpcClient.Call("CoordinatorService.ReleaseLock", req, &resp)
	return &resp, err
}

func (c *Client) Close() {
	c.rpcClient.Close()
	// and free up session
}

func main() {
	clientCfg, _ := configs.LoadConfig("client-config.yaml", "client")
	parsedCfgClient = clientCfg.(configs.ClientConfig)

	client, err := NewRPCClient(configs.DefServerConf().Address, configs.DefServerConf().Host, "client-1")
	if err != nil {
		util.Log().Fatal("Failed to create RPC client: %v", err)
	}
	defer client.Close()

	util.Log().Info("Client connected to server at %s:%d", configs.DefServerConf().Host, configs.DefServerConf().Address)

	resp, err := client.AcquireLock("example-resource", time.Duration(parsedCfgClient.HeartbeatPollingSeconds)*time.Second)
	if err != nil {
		util.Log().Error("Failed to acquire lock: %v", err)
	}
	if resp.Resp.Success {
		util.Log().Info("Lock acquired successfully: %v", resp)

		// do something
		time.Sleep(3 * time.Second)
		releaseResp, err := client.ReleaseLock("example-resource", resp.Token)
		if err != nil {
			util.Log().Error("Failed to release lock: %v", err)
		} else if !releaseResp.Resp.Success {
			util.Log().Warn("Failed to release lock: %s", releaseResp.Resp.Message)
		}
		util.Log().Info("Lock released successfully: %v", releaseResp)
	} else {
		util.Log().Warn("Failed to acquire lock: %s", resp.Resp.Message)
	}

	util.Log().Info("Client operations completed. Keeping it alive for 30 seconds to observe heartbeats.")
	time.Sleep(30 * time.Second)
}
