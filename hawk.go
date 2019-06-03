package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-badger"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/multiformats/go-multiaddr"

	_ "github.com/mattn/go-sqlite3"
	_ "github.com/multiformats/go-multiaddr-dns"
)

const WORKERS = 8

var (
	dumpFlg = flag.String("dump", "", "dumps database to an ndjson file")
	update  = flag.Bool("update", false, "update existing peers")
)

const createSql = `
	CREATE TABLE IF NOT EXISTS peers
	(
		id    TEXT NOT NULL PRIMARY KEY,
		agent TEXT NOT NULL,
		proto TEXT NOT NULL
	);
	
	CREATE TABLE IF NOT EXISTS addrs
	(
		peer_id TEXT NOT NULL,
		addr    TEXT NOT NULL,
		FOREIGN KEY (peer_id) REFERENCES peers (id)
	);
	
	CREATE TABLE IF NOT EXISTS protocols
	(
		peer_id  TEXT NOT NULL,
		protocol TEXT NOT NULL,
		FOREIGN KEY (peer_id) REFERENCES peers (id)
	);
	
	CREATE INDEX IF NOT EXISTS protocol ON protocols (protocol);
	CREATE INDEX IF NOT EXISTS addr ON addrs (addr);
	`

type logOutput struct {
	Id       string
	Agent    string
	Protocol string
	Addrs    []string
	Protos   []string
}

func (l *logOutput) reset() {
	l.Id = ""
	l.Agent = ""
	l.Protocol = ""
	l.Addrs = l.Addrs[0:0]
	l.Protos = l.Protos[0:0]
}

type statement struct {
	sql  string
	stmt *sql.Stmt
}

var statements = struct {
	GetPeer      *statement
	UpsertPeer   *statement
	DeleteAddrs  *statement
	AddAddr      *statement
	DeleteProtos *statement
	AddProtos    *statement
}{
	GetPeer: &statement{
		sql: `
			SELECT count(id) FROM peers where id = ?;
		`,
	},
	UpsertPeer: &statement{
		sql: `
			INSERT INTO peers(id, agent, proto) VALUES (?, ?, ?) 
			ON CONFLICT(id) DO UPDATE SET agent = excluded.agent, proto = excluded.proto;
		`,
	},
	DeleteAddrs: &statement{
		sql: "DELETE FROM addrs WHERE peer_id = ?;",
	},
	DeleteProtos: &statement{
		sql: "DELETE FROM protocols WHERE peer_id = ?;",
	},
	AddAddr: &statement{
		sql: "INSERT INTO addrs(peer_id, addr) VALUES (?, ?)",
	},
	AddProtos: &statement{
		sql: "INSERT INTO protocols(peer_id, protocol) VALUES (?, ?)",
	},
}

type Crawler struct {
	host host.Host
	dht  *dht.IpfsDHT
	ds   datastore.Batching
	id   *identify.IDService

	mu sync.Mutex
	db *sql.DB

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type notifee Crawler

var _ network.Notifiee = (*notifee)(nil)

func NewCrawler() *Crawler {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Crawler{
		ctx:    ctx,
		cancel: cancel,
	}

	c.initDB()
	c.initHost()
	return c
}

func (c *Crawler) initDB() {
	var err error
	db, err := sql.Open("sqlite3", "file:peers.db")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(createSql)
	if err != nil {
		panic(err)
	}
	c.db = db

	stmts := []*statement{
		statements.GetPeer,
		statements.UpsertPeer,
		statements.DeleteAddrs,
		statements.DeleteProtos,
		statements.AddAddr,
		statements.AddProtos,
	}

	for _, s := range stmts {
		if s.stmt, err = db.Prepare(s.sql); err != nil {
			panic(err)
		}
	}
}

func (c *Crawler) initHost() {
	var err error
	c.host, err = libp2p.New(context.Background())
	if err != nil {
		panic(err)
	}

	c.ds, err = badger.NewDatastore("dht.db", nil)
	if err != nil {
		panic(err)
	}

	c.dht = dht.NewDHTClient(c.ctx, c.host, c.ds)

	for _, a := range dht.DefaultBootstrapPeers[4:] {
		pi, err := peer.AddrInfoFromP2pAddr(a)
		if err != nil {
			panic(err)
		}
		ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
		if err = c.host.Connect(ctx, *pi); err != nil {
			fmt.Printf("skipping over bootstrap peer: %s\n", pi.ID.Pretty())
		}
		cancel()
	}

	c.id = identify.NewIDService(c.ctx, c.host)
}

func (c *Crawler) close() {
	c.cancel()
	c.wg.Done()

	if err := c.db.Close(); err != nil {
		fmt.Printf("error while shutting down: %v\n", err)
	}
	if err := c.ds.Close(); err != nil {
		fmt.Printf("error while shutting down: %v\n", err)
	}
}

func (c *Crawler) start() {
	for i := 0; i < WORKERS; i++ {
		c.wg.Add(1)
		go c.worker()
	}

	go c.reporter()

	c.host.Network().Notify((*notifee)(c))
}

func (c *Crawler) worker() {
Work:
	id, err := test.RandPeerID()
	if err != nil {
		panic(err)
	}
	// fmt.Printf("looking for peer: %s\n", id)
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	_, _ = c.dht.FindPeer(ctx, id)
	cancel()
	goto Work
}

func (c *Crawler) reporter() {
	var (
		count int
		row   *sql.Row
		err   error
	)

	for {
		select {
		case <-time.After(10 * time.Second):
			row = c.db.QueryRow("SELECT COUNT(id) FROM peers")
			if err = row.Scan(&count); err != nil {
				panic(err)
			}
			fmt.Printf("--- found %d peers\n", count)
		case <-c.ctx.Done():
			return
		}
	}
}

func (n *notifee) Connected(net network.Network, conn network.Conn) {
	p := conn.RemotePeer()
	pstore := net.Peerstore()

	go func() {
		n.id.IdentifyConn(conn)
		<-n.id.IdentifyWait(conn)

		protos, err := pstore.GetProtocols(p)
		if err != nil {
			panic(err)
		}

		addrs := pstore.Addrs(p)

		agent, err := pstore.Get(p, "AgentVersion")
		switch err {
		case nil:
		case peerstore.ErrNotFound:
			agent = ""
		default:
			panic(err)
		}

		protocol, err := pstore.Get(p, "ProtocolVersion")
		switch err {
		case nil:
		case peerstore.ErrNotFound:
			protocol = ""
		default:
			panic(err)
		}

		line := logOutput{
			Id:       p.Pretty(),
			Agent:    agent.(string),
			Protocol: protocol.(string),
			Addrs: func() (ret []string) {
				for _, a := range addrs {
					ret = append(ret, a.String())
				}
				return ret
			}(),
			Protos: protos,
		}

		j, err := json.Marshal(line)
		if err != nil {
			panic(err)
		}

		fmt.Println(string(j))

		if !*update {
			var cnt int
			row := statements.GetPeer.stmt.QueryRow(p.Pretty())
			if err := row.Scan(&cnt); err != nil {
				panic(err)
			}
			if cnt > 0 {
				fmt.Println("(duplicate peer; skipping)")
				return
			}
		}

		n.mu.Lock()
		defer n.mu.Unlock()

		if _, err = statements.UpsertPeer.stmt.Exec(p.Pretty(), agent, protocol); err != nil {
			panic(err)
		}
		if _, err = statements.DeleteAddrs.stmt.Exec(p.Pretty()); err != nil {
			panic(err)
		}
		for _, addr := range addrs {
			if _, err = statements.AddAddr.stmt.Exec(p.Pretty(), addr.String()); err != nil {
				panic(err)
			}
		}
		if _, err = statements.DeleteProtos.stmt.Exec(p.Pretty()); err != nil {
			panic(err)
		}
		for _, proto := range protos {
			if _, err = statements.AddProtos.stmt.Exec(p.Pretty(), proto); err != nil {
				panic(err)
			}
		}
	}()
}

func (*notifee) Listen(network.Network, multiaddr.Multiaddr)      {}
func (*notifee) ListenClose(network.Network, multiaddr.Multiaddr) {}
func (*notifee) Disconnected(network.Network, network.Conn)       {}
func (*notifee) OpenedStream(network.Network, network.Stream)     {}
func (*notifee) ClosedStream(network.Network, network.Stream)     {}

func main() {

	flag.Parse()

	c := NewCrawler()

	if *dumpFlg != "" {
		fmt.Println("dumping")
		dump(c.db, *dumpFlg)
		return
	}

	c.start()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)

	select {
	case <-ch:
		c.close()
		return
	case <-c.ctx.Done():
		return
	}
}

func dump(db *sql.DB, filename string) {
	var obj logOutput
	var str string

	out, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer out.Close()

	rows, err := db.Query("select * from peers")
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		obj.reset()

		if err := rows.Scan(&obj.Id, &obj.Agent, &obj.Protocol); err != nil {
			panic(err)
		}

		addrRows, err := db.Query("select addr from main.addrs where peer_id = ?", obj.Id)
		if err != nil {
			return
		}
		for addrRows.Next() {
			if err := addrRows.Scan(&str); err != nil {
				panic(err)
			}
			obj.Addrs = append(obj.Addrs, str)
		}

		protoRows, err := db.Query("select protocol from main.protocols where peer_id = ?", obj.Id)
		if err != nil {
			panic(err)
		}
		for protoRows.Next() {
			if err := protoRows.Scan(&str); err != nil {
				panic(err)
			}
			obj.Protos = append(obj.Protos, str)
		}

		j, err := json.Marshal(&obj)
		if err != nil {
			panic(err)
		}

		if _, err = out.WriteString(string(j)); err != nil {
			panic(err)
		}

		if _, err = out.WriteString("\n"); err != nil {
			panic(err)
		}
	}

}
