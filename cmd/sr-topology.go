package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"

	"github.com/golang/glog"
	"wwwin-github.cisco.com/brmcdoug/sr-topology/pkg/arangodb"
	"wwwin-github.cisco.com/brmcdoug/sr-topology/pkg/kafkamessenger"

	_ "net/http/pprof"
)

const (
	// userFile defines the name of file containing base64 encoded user name
	userFile = "./credentials/.username"
	// passFile defines the name of file containing base64 encoded password
	passFile = "./credentials/.password"
	// MAXUSERNAME defines maximum length of ArangoDB user name
	MAXUSERNAME = 256
	// MAXPASS defines maximum length of ArangoDB password
	MAXPASS = 256
)

var (
	msgSrvAddr               string
	dbSrvAddr                string
	dbName                   string
	dbUser                   string
	dbPass                   string
	srnodeCollection         string
	lsprefixCollection       string
	lslinkCollection         string
	lssrv6sidCollection      string
	peerCollection           string
	unicastprefix4Collection string
	lsv4pfxCollection        string
	srtopoCollection         string
)

func init() {
	runtime.GOMAXPROCS(1)
	flag.StringVar(&msgSrvAddr, "message-server", "", "URL to the messages supplying server")
	flag.StringVar(&dbSrvAddr, "database-server", "", "{dns name}:port or X.X.X.X:port of the graph database")
	flag.StringVar(&dbName, "database-name", "", "DB name")
	flag.StringVar(&dbUser, "database-user", "", "DB User name")
	flag.StringVar(&dbPass, "database-pass", "", "DB User's password")
	flag.StringVar(&srnodeCollection, "sr_node_vertex", "sr_node", "sr_node Collection name, default: \"sr_node\"")
	flag.StringVar(&lsprefixCollection, "ls_prefix_vertex", "ls_prefix", "ls_prefix Collection name, default: \"ls_prefix\"")
	flag.StringVar(&lslinkCollection, "ls_link_edge", "ls_link", "ls_link Collection name, default \"ls_link\"")
	flag.StringVar(&lssrv6sidCollection, "ls_srv6_sid_vertex", "ls_srv6_sid", "ls_srv6_sid Collection name, default: \"ls_srv6_sid\"")
	flag.StringVar(&peerCollection, "peer_vertex", "peer", "peer Collection name, default: \"peer\"")
	flag.StringVar(&unicastprefix4Collection, "unicast_prefix_v4_vertex", "unicast_prefix_v4", "unicast prefix v4 Collection name, default: \"unicast_prefix_v4\"")
	flag.StringVar(&lsv4pfxCollection, "lsv4_pfx-name", "lsv4_pfx", "lsv4pfx Collection name, default \"lsv4_pfx\"")
	flag.StringVar(&srtopoCollection, "sr-topology-name", "sr_topology", "Edge Collection name, default \"sr_topology\"")
}

var (
	onlyOneSignalHandler = make(chan struct{})
	shutdownSignals      = []os.Signal{os.Interrupt}
)

func setupSignalHandler() (stopCh <-chan struct{}) {
	close(onlyOneSignalHandler) // panics when called twice

	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return stop
}

func main() {
	flag.Parse()
	_ = flag.Set("logtostderr", "true")

	// validateDBCreds check if the user name and the password are provided either as
	// command line parameters or via files. If both are provided command line parameters
	// will be used, if neither, processor will fail.
	if err := validateDBCreds(); err != nil {
		glog.Errorf("failed to validate the database credentials with error: %+v", err)
		os.Exit(1)
	}

	arangodb.InitializeKafkaNotifier(msgSrvAddr)

	dbSrv, err := arangodb.NewDBSrvClient(dbSrvAddr, dbUser, dbPass, dbName, srnodeCollection, lsprefixCollection,
		lslinkCollection, lssrv6sidCollection, peerCollection, unicastprefix4Collection, lsv4pfxCollection, srtopoCollection)
	if err != nil {
		glog.Errorf("failed to initialize database client with error: %+v", err)
		os.Exit(1)
	}

	if err := dbSrv.Start(); err != nil {
		if err != nil {
			glog.Errorf("failed to connect to database with error: %+v", err)
			//os.Exit(1)
		}
	}

	// Initializing messenger process
	msgSrv, err := kafkamessenger.NewKafkaMessenger(msgSrvAddr, dbSrv.GetInterface())
	if err != nil {
		glog.Errorf("failed to initialize message server with error: %+v", err)
		os.Exit(1)
	}

	msgSrv.Start()

	stopCh := setupSignalHandler()
	<-stopCh

	msgSrv.Stop()
	dbSrv.Stop()

	os.Exit(0)
}

func validateDBCreds() error {
	// Attempting to access username and password files.
	u, err := readAndDecode(userFile, MAXUSERNAME)
	if err != nil {
		if dbUser != "" && dbPass != "" {
			return nil
		}
		return fmt.Errorf("failed to access %s with error: %+v and no username and password provided via command line arguments", userFile, err)
	}
	p, err := readAndDecode(passFile, MAXPASS)
	if err != nil {
		if dbUser != "" && dbPass != "" {
			return nil
		}
		return fmt.Errorf("failed to access %s with error: %+v and no username and password provided via command line arguments", passFile, err)
	}
	dbUser, dbPass = u, p

	return nil
}

func readAndDecode(fn string, max int) (string, error) {
	f, err := os.Open(fn)
	if err != nil {
		return "", err
	}
	defer f.Close()
	l, err := f.Stat()
	if err != nil {
		return "", err
	}
	b := make([]byte, int(l.Size()))
	n, err := io.ReadFull(f, b)
	if err != nil {
		return "", err
	}
	if n > max {
		return "", fmt.Errorf("length of data %d exceeds maximum acceptable length: %d", n, max)
	}
	b = b[:n]

	return string(b), nil
}
