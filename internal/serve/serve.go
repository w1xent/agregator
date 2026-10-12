// Package serve runs HTTP and gRPC servers until a context is done and then
// stops them gracefully.
package serve

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"
)

const shutdownTimeout = 5 * time.Second

// HTTP serves srv until ctx is done.
func HTTP(ctx context.Context, srv *http.Server) error {
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return srv.Shutdown(sctx)
}

// GRPC serves srv on addr until ctx is done. beforeStop is called first,
// so long-living streams can finish; then the server stops gracefully, or
// hard after a timeout.
func GRPC(ctx context.Context, srv *grpc.Server, addr string, beforeStop func()) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(lis) }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	if beforeStop != nil {
		beforeStop()
	}
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownTimeout):
		srv.Stop()
	}
	return nil
}
