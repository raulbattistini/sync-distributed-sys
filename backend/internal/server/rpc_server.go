package server

import (
	"fmt"
	"go-rpc/internal/coordinator"
	"go-rpc/internal/util"
	"net"
	"net/rpc"
)

type CoordinatorService struct {
	coord *coordinator.Coordinator
}

func NewCoordinatorService(coord *coordinator.Coordinator) *CoordinatorService {
	return &CoordinatorService{
		coord: coord,
	}
}

func (cs *CoordinatorService) RegisterSession(req coordinator.RegisterSessionRequest, resp *coordinator.RegisterSessionResponse) error {
	sessionID, expiresAt := cs.coord.RegisterSession(req.ClientID)
	resp.SessionID = sessionID
	resp.ExpiresAt = expiresAt
	return nil
}

func (cs *CoordinatorService) Heartbeat(req coordinator.HeartbeatRequest, resp *coordinator.HeartbeatResponse) error {
	success, expiresAt := cs.coord.Heartbeat(req.SessionID)
	resp.Resp.Success = success
	resp.ExpiresAt = expiresAt
	return nil
}

func (cs *CoordinatorService) AcquireLock(req coordinator.AcquireLockRequest, resp *coordinator.AcquireLockResponse) error {
	*resp = cs.coord.AcquireLock(req)
	return nil
}

func (cs *CoordinatorService) ReleaseLock(req coordinator.ReleaseLockRequest, resp *coordinator.ReleaseLockResponse) error {
	*resp = cs.coord.ReleaseLock(req)
	return nil
}

// should be on init folder
func InitRPCServer(addr int, host string, coord *coordinator.Coordinator) error {
	service := NewCoordinatorService(coord)
	rpc.Register(service)

	netAddress := fmt.Sprintf("%s:%d", host, addr)
	listener, err := net.Listen("tcp", netAddress)
	if err != nil {
		return err
	}

	util.Log().Info("RPC Server listening on %s", netAddress)

	for {
		conn, err := listener.Accept()
		if err != nil {
			util.Log().Error("Error on Accept Listener: %v", err)
			continue
		}

		go rpc.ServeConn(conn)
	}

}
