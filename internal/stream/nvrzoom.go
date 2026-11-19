package stream

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/VoltTech21/reostream/internal/baichuan"
	"github.com/VoltTech21/reostream/internal/hub"
	"github.com/VoltTech21/reostream/internal/ts"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/nareix/joy4/codec/h264parser"
	"github.com/nareix/joy4/format/flv/flvio"
)

func runNVRZoom(ctx context.Context, cfg Config, h *hub.Hub) (err error) {
	host := cfg.Address
	if name, _, e := net.SplitHostPort(host); e == nil {
		host = name
	}
	return readNVRZoom(ctx, cfg, h, net.JoinHostPort(host, "443"))
}

func readNVRZoom(ctx context.Context, cfg Config, h *hub.Hub, httpsAddr string) (err error) {
	// Isolate malformed FLV input, as the native RTSP reader does.
	defer func() {
		if recover() != nil {
			err = errors.New("invalid recorder telephoto stream")
		}
	}()
	quality := "main"
	if cfg.Stream == "zoom_sub" {
		quality = "sub"
	}
	query := url.Values{"port": {"1935"}, "app": {"bcs"}, "stream": {fmt.Sprintf("channel%d_telephoto_%s.bcs", cfg.Channel, quality)}, "user": {cfg.Username}, "password": {cfg.Password}}
	endpoint := url.URL{Scheme: "https", Host: httpsAddr, Path: "/flv", RawQuery: query.Encode()}
	transport := &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion: tls.VersionTLS12,
		// The recorder has a self-signed certificate. Authenticate the exact configured leaf.
		InsecureSkipVerify: true, VerifyConnection: func(state tls.ConnectionState) error {
			digest := sha256.Sum256(state.PeerCertificates[0].Raw)
			if !strings.EqualFold(hex.EncodeToString(digest[:]), cfg.NVRZoomPin) {
				return errors.New("recorder TLS certificate fingerprint mismatch")
			}
			return nil
		},
	}}
	defer transport.CloseIdleConnections()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timeout := cfg.MediaTimeout
	if timeout <= 0 {
		timeout = DefaultMediaTimeout
	}
	watchdog := time.AfterFunc(timeout, cancel)
	defer watchdog.Stop()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return errors.New("invalid recorder HTTPS address")
	}
	client := http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		// net/http wraps errors with the request URL, which carries recorder credentials.
		var uerr *url.Error
		if errors.As(err, &uerr) {
			return fmt.Errorf("recorder HTTPS: %w", uerr.Err)
		}
		return errors.New("recorder HTTPS request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("recorder HTTPS returned %d", resp.StatusCode)
	}
	reader := bufio.NewReader(resp.Body)
	var header [flvio.FileHeaderLength]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}
	_, skip, err := flvio.ParseFileHeader(header[:])
	if err != nil {
		return err
	}
	if _, err := reader.Discard(skip); err != nil {
		return err
	}
	var video h264parser.CodecData
	out := tsOutputFor(h)
	out.connected()
	var mux *ts.Muxer
	for {
		var header [flvio.TagHeaderLength]byte
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			return err
		}
		tag, timestamp, size, err := flvio.ParseTagHeader(header[:])
		if err != nil {
			return err
		}
		if tag.Type != flvio.TAG_VIDEO {
			if _, err := reader.Discard(size + 4); err != nil {
				return err
			}
			continue
		}
		if size < 5 || size > h264.MaxAccessUnitSize+5 {
			return errors.New("invalid recorder FLV video tag size")
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(reader, data); err != nil {
			return err
		}
		if _, err := reader.Discard(4); err != nil {
			return err
		}
		if data[0]&15 != flvio.VIDEO_H264 {
			return errors.New("recorder telephoto requires H.264")
		}
		// RLC-81MA marks P frames as disposable inter frames (0x37).
		// Both inter frame types have the same five-byte AVC header.
		key := data[0]>>4 == flvio.FRAME_KEY
		switch data[1] {
		case flvio.AVC_SEQHDR:
			video, err = h264parser.NewCodecDataFromAVCDecoderConfRecord(data[5:])
			if err != nil {
				return err
			}
			if video.RecordInfo.LengthSizeMinusOne != 3 {
				return errors.New("unsupported recorder AVC length size")
			}
			continue
		case flvio.AVC_NALU:
			if video.Record == nil {
				continue
			}
		default:
			continue
		}
		cts := int32(binary.BigEndian.Uint32(data[1:5])<<8) >> 8
		var au h264.AVCC
		if err := au.Unmarshal(data[5:]); err != nil {
			return err
		}
		watchdog.Reset(timeout)
		kind := baichuan.FramePFrame
		if key {
			kind = baichuan.FrameIFrame
			au = append(h264.AVCC{video.SPS(), video.PPS()}, au...)
		}
		if mux == nil {
			if !key {
				continue
			}
			h.MarkConnected()
			if cs, ok := cfg.Sink.(ConnectionSink); ok {
				cs.Connected()
			}
		}
		f := baichuan.Frame{Kind: kind, Codec: "H264", Width: video.Width(), Height: video.Height(), Micros: uint32(timestamp+cts) * 1000, Data: annexB(au)}
		if err := publishNativeVideo(cfg, h, out, &mux, f); err != nil {
			return err
		}
	}
}
