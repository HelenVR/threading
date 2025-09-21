- go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
- protoc --go_out=. --go_opt=paths=source_relative appsinstalled.proto

go mod init memc_loader
go get github.com/bradfitz/gomemcache/memcache
go get google.golang.org/protobuf/reflect/protoreflect

go build -o memc_loader main.go appsinstalled.pb.go
./memc_loader -pattern="/path tofiles/*.tsv.gz"