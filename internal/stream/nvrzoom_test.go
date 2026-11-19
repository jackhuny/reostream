package stream

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/VoltTech21/reostream/internal/hub"
)

func TestNVRZoomRecorderRequest(t *testing.T) {
	for _, tc := range []struct{ stream, want string }{{"zoom", "channel6_telephoto_main.bcs"}, {"zoom_sub", "channel6_telephoto_sub.bcs"}} {
		t.Run(tc.stream, func(t *testing.T) {
			hit := make(chan string, 1)
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hit <- r.URL.Query().Get("stream")
				http.Error(w, "unavailable", 503)
			}))
			defer srv.Close()
			cfg := Config{Address: "localhost:9000", Channel: 6, Stream: tc.stream, Password: "test-secret", NVRZoomPin: fmt.Sprintf("%x", sha256.Sum256(srv.Certificate().Raw))}
			err := readNVRZoom(context.Background(), cfg, hub.New(2), srv.Listener.Addr().String())
			if err == nil || err.Error() != "recorder HTTPS returned 503" {
				t.Fatalf("unexpected error %v", err)
			}
			select {
			case got := <-hit:
				if got != tc.want {
					t.Errorf("requested %q, want %q", got, tc.want)
				}
			default:
				t.Fatal("recorder received no request")
			}
		})
	}
}
