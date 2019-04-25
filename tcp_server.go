package main

import (
	"fmt"
	"net"
	"os"
)

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

type Server struct {
	listener         net.Listener
	backend_conn_mgr *BackendConnectionMgr
	backend_conn_cfg *BackendConnectionCfg
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
	fmt.Println(self.conn_list)
	self.last_used_idx = (self.last_used_idx + 1) % len(self.conn_list)
	fmt.Println("Last used index", self.last_used_idx)
	return self.conn_list[self.last_used_idx]
}

func CreateServer(backend_servers []string) *Server {
	service := ":1200"
	tcpAddr, err := net.ResolveTCPAddr("tcp", service)
	checkError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	bkend_server_cfg := CreateBackendConfig(backend_servers)
	bkend_conn_mgr := CreateBackendConnMgr(bkend_server_cfg)

	return &Server{
		listener:         listener,
		backend_conn_cfg: bkend_server_cfg,
		backend_conn_mgr: bkend_conn_mgr,
	}
}

func (self *Server) Start() {
	// Run forever
	for {
		conn, err := self.listener.Accept()
		checkError(err)
		backend_conn := self.backend_conn_mgr.GetNext()
		go self.HandleReadFromClient(conn, *backend_conn)
		go self.HandleReadFromBackend(conn, *backend_conn)
	}
}

func (self *Server) HandleReadFromClient(client_conn net.Conn, backend_conn net.Conn) {
	for {
		buffer := make([]byte, 512)
		rd_n, err := client_conn.Read(buffer)
		checkError(err)
		if rd_n == 0 {
			continue
		}
		fmt.Println("Read data from client", buffer)

		_, err = backend_conn.Write(buffer[:rd_n])
		checkError(err)
	}
}

func (self *Server) HandleReadFromBackend(client_conn net.Conn, backend_conn net.Conn) {
	for {
		buffer := make([]byte, 512)
		rd_n, err := backend_conn.Read(buffer)
		checkError(err)
		if rd_n == 0 {
			continue
		}
		fmt.Println("Read data from the backend", buffer)

		_, err = client_conn.Write(buffer[:rd_n])
		checkError(err)
	}
}

func main() {
	server := CreateServer([]string{":6969"})
	server.Start()
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
