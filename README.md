## Init Workspace
```
go mod init github.com/wuxl-lang/proglog
```

## Install Dependency

```
go get github.com/gorilla/mux
go get github.com/stretchr/testify
go get github.com/tysontate/gommap
go get google.golang.org/protobuf/...@v1.25.0
```

### gRPC
```
go get google.golang.org/grpc@v1.32.0
go get google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.0.0

```

### CFSSL
```
go get github.com/cloudflare/cfssl/cmd/cfssl@v1.4.1
go get github.com/cloudflare/cfssl/cmd/cfssljson@v1.4.1
```

cfssl to sign, verify, and bundle TLS certificates and output the results as JSON.
cfssljson to take that JSON output and split them into separate key, certificate, CSR, and bundle files.

### ACL
```
go get github.com/casbin/casbin@v1.9.1
```