package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/wuxl-lang/proglog/api/v1"
	"github.com/wuxl-lang/proglog/config"
	"github.com/wuxl-lang/proglog/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var test_record *api.Record = &api.Record{
	Value: []byte("Hello World"),
}

func TestServer(t *testing.T) {
	cases := map[string]func(t *testing.T, client api.LogClient, cfg *Config){
		"test produce consume":         testProduceConsume,
		"test consume beyond boundary": testConsumePastBoundary,
		"test stream":                  testProduceConsumeStream,
	}

	for scenario, fn := range cases {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t)
			defer teardown()

			fn(t, client, config)
		})
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, cfg *Config) {
	ctx := context.Background()

	// Produce a record
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: test_record,
		},
	)
	require.NoError(t, err)

	// Consume the record
	consume, err := client.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: produce.Offset,
		},
	)
	require.NoError(t, err)

	require.Equal(t, test_record.Value, consume.Record.Value, "Produced and consumed should be equal.")
	require.Equal(t, uint64(0), consume.Record.Offset, "Only one record is proced, offset is 0")
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, cfg *Config) {
	ctx := context.Background()

	// Produce a record
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: test_record,
		},
	)
	require.NoError(t, err)

	// Consume the record with offset, which is out of the boundary
	consume, err := client.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: produce.Offset + 1,
		},
	)
	require.Nil(t, consume, "Nothing is consumed")

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, got, want, "The code in response should be the specific error")
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, cfg *Config) {
	ctx := context.Background()

	records := []*api.Record{{
		Value: []byte("first message"),
	}, {
		Value: []byte("second message"),
	}}

	// Bidirectional stream
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		// Produce record sequentially
		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)

			require.Equal(t, uint64(offset), res.Offset, "Offset should be sequence of produce")
		}
	}

	{
		stream, err := client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{
				Offset: 0, // read all records
			},
		)
		require.NoError(t, err)

		// Read all records from 0 offset
		for offset, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record.Value, record.Value, "Content should be the same, as it is produced with the same sequence")
			require.Equal(t, res.Record.Offset, uint64(offset), "Offset should be the same, as it is produced with the same sequence")
		}
	}
}

func setupTest(t *testing.T) (client api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	// Set up listener
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// Set up Log
	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}

	// Set up server
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)

	serverCreds := credentials.NewTLS(serverTLSConfig)

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	// Set up client
	// Config client's TLS credentialls with CA as client's Root CA, which will use to verify the server.
	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ClientCertFile,
		KeyFile:  config.ClientKeyFile,
		CAFile:   config.CAFile,
	})
	require.NoError(t, err)

	clientCreds := credentials.NewTLS(clientTLSConfig)

	// Use credentials for connection
	cc, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(clientCreds))
	require.NoError(t, err)

	client = api.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}
}
