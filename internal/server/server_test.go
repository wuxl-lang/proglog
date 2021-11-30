package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/wuxl-lang/proglog/api/v1"
	"github.com/wuxl-lang/proglog/config"
	auth "github.com/wuxl-lang/proglog/internal/auth"
	"github.com/wuxl-lang/proglog/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var test_record *api.Record = &api.Record{
	Value: []byte("Hello World"),
}

func TestServer(t *testing.T) {
	cases := map[string]func(t *testing.T, rootClient api.LogClient, nobodyClient api.LogClient, cfg *Config){
		"test produce consume":         testProduceConsume,
		"test consume beyond boundary": testConsumePastBoundary,
		"test stream":                  testProduceConsumeStream,
		"test unauthorize":             testUnauthroized,
	}

	for scenario, fn := range cases {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t)
			defer teardown()

			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func testProduceConsume(t *testing.T, client, _ api.LogClient, cfg *Config) {
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

func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, cfg *Config) {
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

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, cfg *Config) {
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

func testUnauthroized(t *testing.T, _, client api.LogClient, cfg *Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: test_record,
	})

	if produce != nil {
		t.Fatalf("produce response should be nil")
	}

	gotCode := status.Code(err)
	if gotCode != codes.PermissionDenied {
		t.Fatalf("go code: %d, want: %d", gotCode, codes.PermissionDenied)
	}

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: 0,
	})

	if consume != nil {
		t.Fatalf("consume response should be nil")
	}

	gotCode = status.Code(err)
	if gotCode != codes.PermissionDenied {
		t.Fatalf("got code: %d, want %d", gotCode, codes.PermissionDenied)
	}
}

func setupTest(t *testing.T) (rootClient api.LogClient, nobodyClient api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	// Set up listener
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// Set up Log
	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	// Set up authorizer
	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}

	// Set up server
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile, // CerFile and KeyFile is set to allow client to verify
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
	rootConn, rootClient, _ := newClient(t, l.Addr().String(), config.RootClientCertFile, config.RootClientKeyFile)
	nobodyConn, nobodyClient, _ := newClient(t, l.Addr().String(), config.NobodyClientCertFile, config.NobodyClientKeyFile)

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		clog.Remove()
	}
}

func newClient(t *testing.T, address, crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
	// Config client's TLS credentialls with CA as client's Root CA, which will use to verify the server.
	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: crtPath,
		KeyFile:  keyPath, // CertFile and KeyFile is set to allow server to verify
		CAFile:   config.CAFile,
		Server:   false,
	})
	require.NoError(t, err)

	// Use credentials for connection
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	conn, err := grpc.Dial(address, opts...)
	require.NoError(t, err)

	// New client
	client := api.NewLogClient(conn)

	return conn, client, opts
}
