// Copyright Fuzamei Corp. 2018 All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gossip

import (
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"sync"
	"time"

	pb "github.com/33cn/chain33/types"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	pr "google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
)

// Listener the actions
type Listener interface {
	Close1()
	Start()
}

// Start server start
func (l *listener) Start() {
	l.p2pserver.Start()
	go l.server.Serve(l.netlistener)

}

// Close server close
func (l *listener) Close() {
	err := l.netlistener.Close()
	if err != nil {
		log.Error("Close", "netlistener.Close() err", err)
	}
	go l.server.Stop()
	l.p2pserver.Close()
	log.Info("stop", "server", "close")

}

type listener struct {
	server      *grpc.Server
	nodeInfo    *NodeInfo
	p2pserver   *P2pserver
	node        *Node
	netlistener net.Listener
}

// newListener produce a server object
func newListener(protocol string, node *Node) *listener {
Retry:
	log.Info("newListener", "localPort", node.listenPort)
	l, err := net.Listen(protocol, fmt.Sprintf(":%v", node.listenPort))
	if err != nil {
		log.Error("Failed to listen", "Error", err.Error())
		for {
			randPort := rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(65535)
			if int(randPort) == node.listenPort || randPort < 2048 {
				continue
			}
			node.listenPort = int(randPort)
			break
		}
		log.Info("Flush Listen Port", "RandPort", node.listenPort)
		goto Retry
	}

	dl := &listener{
		nodeInfo:    node.nodeInfo,
		node:        node,
		netlistener: l,
	}

	pServer := NewP2pServer()
	pServer.node = dl.node

	//一元拦截器 接口调用之前进行校验拦截
	interceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		//checkAuth
		getctx, ok := pr.FromContext(ctx)
		if !ok {
			return nil, fmt.Errorf("")
		}
		ip, _, err := net.SplitHostPort(getctx.Addr.String())
		if err != nil {
			return nil, err
		}
		if serialNum, ok := latestSerials.Load(ip); ok {
			bn, _ := big.NewInt(1).SetString(serialNum.(string), 10)
			if isRevoke(bn) { //证书被吊销 拒绝接口请求
				return nil, fmt.Errorf("cert %v revoked", serialNum.(string))
			}
		}
		if pServer.node.nodeInfo.blacklist.Has(ip) {
			return nil, fmt.Errorf("blacklist %v no authorized", ip)
		}

		if !auth(ip) {
			log.Error("interceptor", "auth faild", ip)
			//把相应的IP地址加入黑名单中
			pServer.node.nodeInfo.blacklist.Add(ip, int64(3600))
			return nil, fmt.Errorf("auth faild %v  no authorized", ip)

		}
		// Continue processing the request
		return handler(ctx, req)
	}
	//流拦截器
	interceptorStream := func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		getctx, ok := pr.FromContext(ss.Context())
		if !ok {
			log.Error("interceptorStream", "FromContext error", "")
			return fmt.Errorf("stream Context err")
		}
		ip, _, err := net.SplitHostPort(getctx.Addr.String())
		if err != nil {
			return err
		}
		if serialNum, ok := latestSerials.Load(ip); ok {
			bn, _ := big.NewInt(1).SetString(serialNum.(string), 10)
			if isRevoke(bn) { //证书被吊销 拒绝接口请求
				return fmt.Errorf("cert %v revoked", serialNum.(string))
			}
		}

		if pServer.node.nodeInfo.blacklist.Has(ip) {
			return fmt.Errorf("blacklist %v  no authorized", ip)
		}

		if !auth(ip) {
			log.Error("interceptorStream", "auth faild", ip)
			//把相应的IP地址加入黑名单中
			pServer.node.nodeInfo.blacklist.Add(ip, int64(3600))
			return fmt.Errorf("auth faild  %v  no authorized", ip)
		}
		return handler(srv, ss)
	}
	var opts []grpc.ServerOption
	opts = append(opts, grpc.UnaryInterceptor(interceptor), grpc.StreamInterceptor(interceptorStream))
	maxMsgSize := pb.MaxBlockSize + 1024*1024    //最大传输数据 最大区块大小
	msgRecvOp := grpc.MaxRecvMsgSize(maxMsgSize) //设置最大接收数据
	msgSendOp := grpc.MaxSendMsgSize(maxMsgSize) //设置最大发送数据
	kaep := keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second, //只允许不低于10s频率的ping周期
		PermitWithoutStream: true,
	}
	var keepparm keepalive.ServerParameters
	keepparm.Time = 5 * time.Minute
	keepparm.Timeout = 50 * time.Second
	maxStreams := grpc.MaxConcurrentStreams(1000)
	keepOp := grpc.KeepaliveParams(keepparm)
	StatsOp := grpc.StatsHandler(&statshandler{})
	opts = append(opts, msgRecvOp, msgSendOp, grpc.KeepaliveEnforcementPolicy(kaep), keepOp, maxStreams, StatsOp)
	if node.nodeInfo.servCreds != nil {
		opts = append(opts, grpc.Creds(node.nodeInfo.servCreds))
	}

	dl.server = grpc.NewServer(opts...)
	dl.p2pserver = pServer
	pb.RegisterP2PgserviceServer(dl.server, pServer)

	return dl
}

type statshandler struct{}

func (h *statshandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return context.WithValue(ctx, connCtxKey{}, info)
}

func (h *statshandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return ctx
}

func (h *statshandler) HandleConn(ctx context.Context, s stats.ConnStats) {
	if ctx == nil {
		return
	}
	tag, ok := getConnTagFromContext(ctx)
	if !ok {
		log.Error("can not get conn tag")
		return
	}

	ip, _, err := net.SplitHostPort(tag.RemoteAddr.String())
	if err != nil {
		return
	}
	connsMutex.Lock()
	defer connsMutex.Unlock()
	if _, ok := conns[ip]; !ok {
		conns[ip] = 0
	}
	switch s.(type) {
	case *stats.ConnBegin:
		conns[ip] = conns[ip] + 1
	case *stats.ConnEnd:
		conns[ip] = conns[ip] - 1
		if conns[ip] <= 0 {
			delete(conns, ip)
		}
		log.Debug("ip connend", "ip", ip, "n", conns[ip])
	default:
		log.Error("illegal ConnStats type")
	}
}

// HandleRPC 为空.
func (h *statshandler) HandleRPC(ctx context.Context, s stats.RPCStats) {}

type connCtxKey struct{}

var connsMutex sync.Mutex

var conns = make(map[string]uint)

func getConnTagFromContext(ctx context.Context) (*stats.ConnTagInfo, bool) {
	tag, ok := ctx.Value(connCtxKey{}).(*stats.ConnTagInfo)
	return tag, ok
}

func auth(checkIP string) bool {
	connsMutex.Lock()
	defer connsMutex.Unlock()
	count, ok := conns[checkIP]
	if ok && count > maxSamIPNum {
		log.Error("AuthCheck", "sameIP num:", count, "checkIP:", checkIP, "diffIP num:", len(conns))
		return false
	}

	return true
}
