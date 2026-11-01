package registry

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// KeepAlive registers inst, renews its lease every interval until ctx is
// cancelled, then deregisters it. Run it in a goroutine next to your server.
//
// If a heartbeat returns ErrNotFound, because the registry restarted or the
// lease expired, the instance registers again. Other heartbeat errors are
// logged and retried on the next tick. Only the first registration's error
// is returned.
func KeepAlive(ctx context.Context, reg Registry, inst Instance, interval time.Duration) error {
	if err := reg.Register(ctx, inst); err != nil {
		return err
	}
	log := slog.With("service", inst.Service, "instance", inst.ID)
	log.Info("registered", "address", inst.Address)

	defer func() {
		// ctx is already done, so deregister on a fresh deadline.
		dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := reg.Deregister(dctx, inst.Service, inst.ID); err != nil && !errors.Is(err, ErrNotFound) {
			log.Warn("deregister failed", "err", err)
			return
		}
		log.Info("deregistered")
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		err := reg.Heartbeat(ctx, inst.Service, inst.ID)
		switch {
		case err == nil || ctx.Err() != nil:
		case errors.Is(err, ErrNotFound):
			log.Info("lease lost, registering again")
			if err := reg.Register(ctx, inst); err != nil {
				log.Warn("re-register failed", "err", err)
			}
		default:
			log.Warn("heartbeat failed", "err", err)
		}
	}
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
