package arangodb

import (
	"context"
	"encoding/json"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/jalapeno/topology/pkg/dbclient"
	notifier "github.com/jalapeno/topology/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/message"
	"github.com/sbezverk/gobmp/pkg/tools"
)

type arangoDB struct {
	dbclient.DB
	*ArangoConn
	stop            chan struct{}
	srnode          driver.Collection
	lsprefix        driver.Collection
	lslink          driver.Collection
	lssrv6          driver.Collection
	peer            driver.Collection
	unicastprefixv4 driver.Collection
	lsv4pfx         driver.Collection
	srtopo          driver.Graph
	graph           driver.Collection
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname, srnode string, lsp string, lsl string, lssrv6 string, peer string, upv4 string, lsv4pfx string, srtopo string) (dbclient.Srv, error) {
	if err := tools.URLAddrValidation(arangoSrv); err != nil {
		return nil, err
	}
	arangoConn, err := NewArango(ArangoConfig{
		URL:      arangoSrv,
		User:     user,
		Password: pass,
		Database: dbname,
	})
	if err != nil {
		return nil, err
	}
	arango := &arangoDB{
		stop: make(chan struct{}),
	}
	arango.DB = arango
	arango.ArangoConn = arangoConn

	// Check if collections exists, if not fail as Jalapeno topology is not running
	glog.V(5).Infof("checking collections")
	arango.srnode, err = arango.db.Collection(context.TODO(), srnode)
	if err != nil {
		return nil, err
	}
	arango.lsprefix, err = arango.db.Collection(context.TODO(), lsp)
	if err != nil {
		return nil, err
	}
	arango.lslink, err = arango.db.Collection(context.TODO(), lsl)
	if err != nil {
		return nil, err
	}
	arango.lssrv6, err = arango.db.Collection(context.TODO(), lssrv6)
	if err != nil {
		return nil, err
	}
	arango.peer, err = arango.db.Collection(context.TODO(), peer)
	if err != nil {
		return nil, err
	}
	arango.unicastprefixv4, err = arango.db.Collection(context.TODO(), upv4)
	if err != nil {
		return nil, err
	}

	// check for ipv4 topology graph
	found, err := arango.db.GraphExists(context.TODO(), srtopo)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Graph(context.TODO(), srtopo)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}
	// create graph
	var edgeDefinition driver.EdgeDefinition
	edgeDefinition.Collection = "sr_topology"
	edgeDefinition.From = []string{"ls_node"}
	edgeDefinition.To = []string{"ls_node", "peer", "unicast_prefix_v4"}
	var options driver.CreateGraphOptions
	options.OrphanVertexCollections = []string{"peer", "ls_prefix"}
	options.EdgeDefinitions = []driver.EdgeDefinition{edgeDefinition}

	arango.srtopo, err = arango.db.CreateGraph(context.TODO(), srtopo, &options)
	if err != nil {
		return nil, err
	}
	// check if graph exists, if not fail as processor has failed to create graph
	arango.graph, err = arango.db.Collection(context.TODO(), "sr_topology")
	if err != nil {
		return nil, err
	}

	// check for lspfx collection
	found, err = arango.db.CollectionExists(context.TODO(), lsv4pfx)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), lsv4pfx)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}
	// create ls_pfx collection
	var lsv4pfx_options = &driver.CreateCollectionOptions{ /* ... */ }
	glog.V(5).Infof("lspfx not found, creating")
	arango.lsv4pfx, err = arango.db.CreateCollection(context.TODO(), "lsv4_pfx", lsv4pfx_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.lsv4pfx, err = arango.db.Collection(context.TODO(), lsv4pfx)
	if err != nil {
		return nil, err
	}

	return arango, nil
}

func (a *arangoDB) Start() error {
	if err := a.loadEdge(); err != nil {
		return err
	}
	glog.Infof("Connected to arango database, starting monitor")
	go a.monitor()

	return nil
}

func (a *arangoDB) Stop() error {
	close(a.stop)

	return nil
}

func (a *arangoDB) GetInterface() dbclient.DB {
	return a.DB
}

func (a *arangoDB) GetArangoDBInterface() *ArangoConn {
	return a.ArangoConn
}

func (a *arangoDB) StoreMessage(msgType dbclient.CollectionType, msg []byte) error {
	event := &notifier.EventMessage{}
	if err := json.Unmarshal(msg, event); err != nil {
		return err
	}
	event.TopicType = msgType
	switch msgType {
	case bmp.LSLinkMsg:
		return a.lsLinkHandler(event)
	//case bmp.LSNodeMsg:
	//	return a.lsNodeHandler(event)
	case bmp.LSPrefixMsg:
		return a.lsPrefixHandler(event)
	// case bmp.LSPrefixMsg:
	// 	return a.lsv4PfxHandler(event)
	case bmp.UnicastPrefixV4Msg:
		return a.unicastPrefixHandler(event)
		//case bmp.PeerStateChangeMsg:
		//	return a.peerHandler(event)
	}

	return nil
}

func (a *arangoDB) monitor() {
	for {
		select {
		case <-a.stop:
			// TODO Add clean up of connection with Arango DB
			return
		}
	}
}

func (a *arangoDB) loadEdge() error {
	ctx := context.TODO()
	query := "FOR d IN " + a.lslink.Name() + " filter d.mt_id_tlv.mt_id != 2 RETURN d"
	cursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.LSLink
		meta, err := cursor.ReadDocument(ctx, &p)
		//glog.V(5).Infof("processing lslink document: %+v", p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		glog.V(5).Infof("get ipv4 ls_link: %s", p.Key)
		if err := a.processEdge(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}
	//ctx = context.TODO()
	epe_query := "FOR d IN " + a.lslink.Name() + " filter d.protocol_id == 7 RETURN d"
	cursor, err = a.db.Query(ctx, epe_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.LSLink
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		glog.V(5).Infof("get ipv4 epe_link: %s", p.Key)
		if err := a.processEPE(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}
	epe_prefix_query := "for d in " + a.peer.Name() + " RETURN d"
	cursor, err = a.db.Query(ctx, epe_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.PeerStateChange
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		glog.V(5).Infof("get ipv4 eBGP peer: %s", p.Key)
		if err := a.processEdgeByPeer(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	ibgp_prefix_query := "for l in " + a.srnode.Name() + " return l"
	cursor, err = a.db.Query(ctx, ibgp_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p SRNode
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		glog.V(5).Infof("get ipv4 iBGP prefixes attach to lsnode: %s", p.Key)
		if err := a.processIBGP(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	lsprefix_query := "for l in " + a.lsprefix.Name() +
		" filter l.mt_id_tlv.mt_id != 2 && l.prefix_len != 30 && " +
		"l.prefix_len != 31 && l.prefix_len != 32 && l.prefix_metric == null return l"
	cursor, err = a.db.Query(ctx, lsprefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.LSPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		if err := a.processLSPrefix(ctx, meta.Key, &p); err != nil {
			glog.Errorf("Failed to process LSPrefix %s with error: %+v", p.ID, err)
		}
	}

	lsv4pfx_insert := "for p in ls_prefix filter p.mt_id_tlv.mt_id != 2 insert p in lsv4_pfx"
	cursor, err = a.db.Query(ctx, lsv4pfx_insert, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	lsv4pfx_remove := "for p in lsv4_pfx for l in ls_link filter p.prefix == l.local_link_ip remove p in lsv4_pfx"
	cursor, err = a.db.Query(ctx, lsv4pfx_remove, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// lsv4pfx_query := "for l in lsv4_pfx return l"
	// cursor, err = a.db.Query(ctx, lsv4pfx_query, nil)
	// if err != nil {
	// 	return err
	// }
	// defer cursor.Close()
	// for {
	// 	var p message.LSPrefix
	// 	meta, err := cursor.ReadDocument(ctx, &p)
	// 	if driver.IsNoMoreDocuments(err) {
	// 		break
	// 	} else if err != nil {
	// 		return err
	// 	}
	// 	if err := a.processLSv4Pfx(ctx, meta.Key, &p); err != nil {
	// 		glog.Errorf("Failed to process LSv4Prefix %s with error: %+v", p.ID, err)
	// 	}

	// }

	return nil
}
