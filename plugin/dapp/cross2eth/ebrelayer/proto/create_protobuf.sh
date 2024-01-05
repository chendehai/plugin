#!/bin/sh
# proto生成命令，将pb.go文件生成到types/目录下, chain33_path支持引用chain33框架的proto文件
chain33_path=$(go list -f '{{.Dir}}' "github.com/33cn/chain33")
# 老版本的protoc采用下面这个命令（v3.17.3）
#protoc --go_out=plugins=grpc:../types ./*.proto --proto_path=. --proto_path="${chain33_path}/types/proto/"
# 新版本的protoc采用下面这个命令(v4.25.1) m1芯片的protoc只能为新版本的
protoc --go_out=. --go_opt=paths=import \
    --go-grpc_out=. --go-grpc_opt=paths=import \
    ./*.proto