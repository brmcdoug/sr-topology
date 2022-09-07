package arangodb

import (
	"context"
	"strconv"
	"strings"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/base"
	"github.com/sbezverk/gobmp/pkg/message"
)

// processEPE. processes a single EPE ls_link connection which is a unidirectional edge from egress node to external peer (vertices).
func (a *arangoDB) processEPE(ctx context.Context, key string, e *message.LSLink) error {
	if e.ProtocolID != base.BGP {
		return nil
	}
	if strings.Contains(e.Key, ":") {
		return nil
	}
	query := "FOR d IN " + a.srnode.Name() +
		" filter d.router_id == " + "\"" + e.BGPRouterID + "\"" +
		" filter d.domain_id == " + strconv.Itoa(int(e.DomainID))
	query += " return d"
	glog.Infof("query lsnode for router id matching lslink bgp_router_id: %+v, local_link_ip: %+v", e.BGPRouterID, e.LocalLinkIP)
	lcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}

	defer lcursor.Close()
	var ln SRNode
	lm, err := lcursor.ReadDocument(ctx, &ln)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}
	glog.Infof("ls_node: %+v to correlate with EPE ls_link: %+v", ln.Key, e.Key)

	query = "FOR d IN peer" + //a.peer.Name() +
		" filter d.remote_bgp_id == " + "\"" + e.BGPRemoteRouterID + "\"" +
		" filter d.remote_ip == " + "\"" + e.RemoteLinkIP + "\""
	query += " return d"
	glog.Infof("query peer collection for router id matching lslink bgp_remote_router_id: %+v and remotelinkIP: %+v", e.BGPRemoteRouterID, e.RemoteLinkIP)
	rcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer rcursor.Close()
	var rn message.PeerStateChange
	rm, err := rcursor.ReadDocument(ctx, &rn)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}
	glog.V(5).Infof("From lsnode: %+v", ln.Key)
	glog.V(5).Infof("To peer: %+v", rn.Key)

	ne := epeEdgeObject{
		Key:             key,
		From:            lm.ID.String(),
		To:              rm.ID.String(),
		ProtocolID:      e.ProtocolID,
		DomainID:        e.DomainID,
		LocalNodeName:   ln.Name,
		RemoteNodeName:  rn.Name,
		LocalLinkIP:     e.LocalLinkIP,
		RemoteLinkIP:    rn.RemoteIP,
		LocalNodeASN:    e.LocalNodeASN,
		RemoteNodeASN:   e.RemoteNodeASN,
		RemoteNodeBGPID: e.BGPRemoteRouterID,
		PeerNodeSID:     e.PeerNodeSID,
		PeerAdjSID:      e.PeerAdjSID,
		PeerSetSID:      e.PeerSetSID,
	}
	var doc driver.DocumentMeta
	if doc, err = a.graph.CreateDocument(ctx, &ne); err != nil {

		if !driver.IsConflict(err) {
			return err
		}
		// The document already exists, updating it with the latest info
		doc, err = a.graph.UpdateDocument(ctx, e.Key, &ne)
		if err != nil {
			return err
		}
	}

	notifyKafka(doc, e.Action)
	return nil
}
