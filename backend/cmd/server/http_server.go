package server

import (
	"context"
	"go-rpc/internal/coordinator"
	"go-rpc/internal/util"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type RpcServHttp struct {
	coord     *coordinator.Coordinator
	ginInst   *gin.Engine
	ginRouter *gin.RouterGroup
}

func NewGinInst(coord *coordinator.Coordinator) *RpcServHttp {
	s := &RpcServHttp{
		coord:   coord,
		ginInst: gin.Default(),
	}
	s.initRoutes()
	return s
	// router := server.Group("/api/v1")
}
func (rs *RpcServHttp) initRoutes() {
	rs.ginRouter = rs.ginInst.Group("/api/v1")

	rs.ginRouter.GET("/sessions", rs.handleGetSessions)
	rs.ginRouter.GET("/sessions/:id/heartbeat", rs.handleHeartbeat)
	rs.ginRouter.POST("/locks/acquire", rs.handleAcquireLock)
	rs.ginRouter.POST("/locks/release", rs.handleReleaseLock)

	rs.ginRouter.GET("/health", rs.handleHealthCheck)
	rs.ginRouter.GET("/stats", rs.handleStats)
}

func (rs *RpcServHttp) ListenAndRun(ctx context.Context, address string) error {
	rs.ginInst = gin.Default()
	rs.initRoutes()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		serviceHttp := &http.Server{
			Addr:    address,
			Handler: rs.ginInst,
		}
		if err := serviceHttp.Shutdown(shutdownCtx); err != nil {
			util.Log().Error("HTTP server forced to shutdown: %v", err)
		}
	}()

	util.Log().Info("Starting HTTP server on %s", address)
	if err := rs.ginInst.Run(address); err != nil {
		util.Log().Error("Failed to start HTTP server: %v", err)
		return err
	}
	return nil
}

func (rs *RpcServHttp) Shutdown() error {
	if rs.ginInst != nil {
		httpService := &http.Server{
			Addr:    "",
			Handler: rs.ginInst,
		}
		if err := httpService.Shutdown(context.Background()); err != nil {
			util.Log().Error("Error shutting down HTTP server: %v", err)
			return err
		}
		util.Log().Info("Shut down HTTP server")
	}
	return nil
}

func (rs *RpcServHttp) handleHealthCheck(c *gin.Context) {}
