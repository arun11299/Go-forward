package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

type ProxiedConnection struct {
	incoming_conn           *net.Conn
	total_incoming_requests uint64
	outgoing_conn           *net.Conn
	total_outgoing_requests uint64
}

// List of backend server connections
type BackendConnectionCfg struct {
	servers []string
}

type BackendConnectionMgr struct {
	config        *BackendConnectionCfg
	conn_map      map[string]net.Conn
	conn_list     []*net.Conn
	last_used_idx int
}

func CreateBackendConfig(servers []string) *BackendConnectionCfg {
	return &BackendConnectionCfg{
		servers: servers,
	}
}

func CreateBackendConnMgr(cfg *BackendConnectionCfg) *BackendConnectionMgr {
	cmap := make(map[string]net.Conn, 0)
	clist := make([]*net.Conn, 0)

	for _, server := range cfg.servers {
		fmt.Println("Connecting to", server)
		conn, err := net.Dial("tcp", server)
		if err != nil {
			fmt.Println("Error while connecting to", server, err.Error())
			continue
		}
		cmap[server] = conn
		clist = append(clist, &conn)
	}

	return &BackendConnectionMgr{
		config:        cfg,
		conn_map:      cmap,
		conn_list:     clist,
		last_used_idx: -1,
	}
}

func (self *BackendConnectionMgr) GetNext() *net.Conn {
	fmt.Println("Length of conn_list", len(self.conn_list))
	fmt.Println(self.conn_list)
	self.last_used_idx = (self.last_used_idx + 1) % len(self.conn_list)
	fmt.Println("Last used index", self.last_used_idx)
	return self.conn_list[self.last_used_idx]
}

func RunLoadBalancer(backend_servers []string) {
	service := ":1200"
	tcpAddr, err := net.ResolveTCPAddr("tcp", service)
	checkError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	bkend_server_cfg := CreateBackendConfig(backend_servers)
	bkend_conn_mgr := CreateBackendConnMgr(bkend_server_cfg)

	// Run forever
	for {
		conn, err := listener.Accept()
		checkError(err)

		go manageProxiedConnection(bkend_conn_mgr, conn)
	}

}

func manageProxiedConnection(conn_mgr *BackendConnectionMgr, client_conn net.Conn) {
	for {
		backend_conn := conn_mgr.GetNext()
		wire_data, err := bufio.NewReader(client_conn).ReadString('\n')
		fmt.Println("Data received from client", wire_data)
		checkError(err)
		wr_bytes, err := (*backend_conn).Write([]byte(wire_data))
		fmt.Println("Bytes written to backend", wr_bytes)
		checkError(err)
	}
}

func main() {
	RunLoadBalancer([]string{":6969", ":6969", ":6969"})
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
