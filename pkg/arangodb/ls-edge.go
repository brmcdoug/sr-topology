package arangodb

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/base"
	"github.com/sbezverk/gobmp/pkg/message"
)

// processEdge processes a single ls_link connection which is a unidirectional edge between two ls_nodes (vertices).
func (a *arangoDB) processEdge(ctx context.Context, key string, e *message.LSLink) error {
	if strings.Contains(e.Key, ":") {
		return nil
	}
	if e.MTID != nil {
		return nil
	}
	// ProtocolID of 7 is Egress Peer Engineering
	if e.ProtocolID == base.BGP {
		return a.processEPE(ctx, key, e)
		//return nil
	}
	// Need to find ls_node object matching ls_link's IGP Router ID
	query := "FOR d IN " + a.srnode.Name() +
		" filter d.igp_router_id == " + "\"" + e.IGPRouterID + "\"" +
		" filter d.domain_id == " + strconv.Itoa(int(e.DomainID)) +
		" filter d.protocol_id == " + strconv.Itoa(int(e.ProtocolID))
	// If OSPFv2 or OSPFv3, then query must include AreaID
	if e.ProtocolID == base.OSPFv2 || e.ProtocolID == base.OSPFv3 {
		query += " filter d.area_id == " + "\"" + e.AreaID + "\""
	}
	query += " return d"
	lcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer lcursor.Close()

	var ln SRNode
	// var lm driver.DocumentMeta
	i := 0
	for ; ; i++ {
		_, err := lcursor.ReadDocument(ctx, &ln)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
	}
	if i == 0 {
		return fmt.Errorf("query %s returned 0 results", query)
	}
	if i > 1 {
		return fmt.Errorf("query %s returned more than 1 result", query)
	}

	query = "FOR d IN " + a.srnode.Name() +
		" filter d.igp_router_id == " + "\"" + e.RemoteIGPRouterID + "\"" +
		" filter d.domain_id == " + strconv.Itoa(int(e.DomainID)) +
		" filter d.protocol_id == " + strconv.Itoa(int(e.ProtocolID))
	// If OSPFv2 or OSPFv3, then query must include AreaID
	if e.ProtocolID == base.OSPFv2 || e.ProtocolID == base.OSPFv3 {
		query += " filter d.area_id == " + "\"" + e.AreaID + "\""
	}
	query += " return d"
	rcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer rcursor.Close()
	i = 0
	var rn SRNode
	for ; ; i++ {
		_, err := rcursor.ReadDocument(ctx, &rn)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
	}
	if i == 0 {
		return fmt.Errorf("query %s returned 0 results", query)
	}
	if i > 1 {
		return fmt.Errorf("query %s returned more than 1 result", query)
	}
	glog.V(6).Infof("Local node -> Protocol: %+v Domain ID: %+v IGP Router ID: %+v",
		ln.ProtocolID, ln.DomainID, ln.IGPRouterID)
	glog.V(6).Infof("Remote node -> Protocol: %+v Domain ID: %+v IGP Router ID: %+v",
		rn.ProtocolID, rn.DomainID, rn.IGPRouterID)

	mtid := 0
	if e.MTID != nil {
		mtid = int(e.MTID.MTID)
	}
	ne := lslinkEdgeObject{
		Key:            key,
		From:           ln.ID,
		To:             rn.ID,
		Link:           e.Key,
		ProtocolID:     e.ProtocolID,
		DomainID:       e.DomainID,
		MTID:           uint16(mtid),
		AreaID:         e.AreaID,
		LocalNodeName:  ln.Name,
		RemoteNodeName: rn.Name,
		LocalIGPID:     e.IGPRouterID,
		RemoteIGPID:    e.RemoteIGPRouterID,
		LocalLinkID:    e.LocalLinkID,
		RemoteLinkID:   e.RemoteLinkID,
		LocalLinkIP:    e.LocalLinkIP,
		RemoteLinkIP:   e.RemoteLinkIP,
		LocalNodeASN:   e.LocalNodeASN,
		RemoteNodeASN:  e.RemoteNodeASN,
		SRv6ENDXSID:    e.SRv6ENDXSID,
		LSAdjSID:       e.LSAdjacencySID,
		PrefixAttrTLVs: rn.PrefixAttrTLVs,
		SID:            rn.SID,
		SRv6SID:        rn.SRv6SID,
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

// func (a *arangoDB) processVertex(ctx context.Context, key string, e *SRNode) error {
// 	if e.ProtocolID == 7 {
// 		return nil
// 	}
// 	// Check if there is an edge with matching to LSNode's e.IGPRouterID, e.AreaID, e.DomainID and e.ProtocolID
// 	query := "FOR d IN " + a.lsnode.Name() +
// 		" filter d.igp_router_id == " + "\"" + e.IGPRouterID + "\"" +
// 		" filter d.domain_id == " + strconv.Itoa(int(e.DomainID)) +
// 		" filter d.protocol_id == " + strconv.Itoa(int(e.ProtocolID))
// 	// If OSPFv2 or OSPFv3, then query must include AreaID
// 	if e.ProtocolID == base.OSPFv2 || e.ProtocolID == base.OSPFv3 {
// 		query += " filter d.area_id == " + "\"" + e.AreaID + "\""
// 	}
// 	query += " return d"
// 	lcursor, err := a.db.Query(ctx, query, nil)
// 	if err != nil {
// 		return err
// 	}
// 	defer lcursor.Close()
// 	var ln message.LSLink
// 	// var lm driver.DocumentMeta
// 	i := 0
// 	for ; ; i++ {
// 		_, err := lcursor.ReadDocument(ctx, &ln)
// 		if err != nil {
// 			if !driver.IsNoMoreDocuments(err) {
// 				return err
// 			}
// 			break
// 		}
// 	}
// 	if i == 0 {
// 		return fmt.Errorf("query %s returned 0 results", query)
// 	}
// 	if i > 1 {
// 		return fmt.Errorf("query %s returned more than 1 result", query)
// 	}

// 	// Check if there is a second ls_ink with with matching to ls_node's e.IGPRouterID, e.AreaID, e.DomainID and e.ProtocolID
// 	query = "FOR d IN " + a.lsnode.Name() +
// 		" filter d.remote_igp_router_id == " + "\"" + e.IGPRouterID + "\"" +
// 		" filter d.domain_id == " + strconv.Itoa(int(e.DomainID)) +
// 		" filter d.protocol_id == " + strconv.Itoa(int(e.ProtocolID))
// 	// If OSPFv2 or OSPFv3, then query must include AreaID
// 	if e.ProtocolID == base.OSPFv2 || e.ProtocolID == base.OSPFv3 {
// 		query += " filter d.area_id == " + "\"" + e.AreaID + "\""
// 	}
// 	query += " return d"
// 	rcursor, err := a.db.Query(ctx, query, nil)
// 	if err != nil {
// 		return err
// 	}
// 	defer rcursor.Close()
// 	var rn message.LSLink
// 	// var lm driver.DocumentMeta
// 	i = 0
// 	for ; ; i++ {
// 		_, err := lcursor.ReadDocument(ctx, &rn)
// 		if err != nil {
// 			if !driver.IsNoMoreDocuments(err) {
// 				return err
// 			}
// 			break
// 		}
// 	}
// 	if i == 0 {
// 		return fmt.Errorf("query %s returned 0 results", query)
// 	}
// 	if i > 1 {
// 		return fmt.Errorf("query %s returned more than 1 result", query)
// 	}

// 	glog.V(6).Infof("Local link: %s", ln.ID)
// 	glog.V(6).Infof("Remote link: %s", rn.ID)

// 	mtid := 0
// 	if rn.MTID != nil {
// 		mtid = int(rn.MTID.MTID)
// 	}
// 	ne := ipv4TopologyObject{
// 		Key:        key,
// 		From:       ln.ID,
// 		To:         rn.ID,
// 		Link:       rn.Key,
// 		ProtocolID: rn.ProtocolID,
// 		DomainID:   rn.DomainID,
// 		MTID:       uint16(mtid),
// 		AreaID:     rn.AreaID,
// 	}

// 	var doc driver.DocumentMeta
// 	if doc, err = a.graph.CreateDocument(ctx, &ne); err != nil {
// 		if !driver.IsConflict(err) {
// 			return err
// 		}
// 		// The document already exists, updating it with the latest info
// 		if _, err = a.graph.UpdateDocument(ctx, e.Key, &ne); err != nil {
// 			return err
// 		}
// 	}

// 	notifyKafka(doc, e.Action)
// 	return nil
// }

// processEdgeRemoval removes a record from Node's graph collection
// since the key matches in both collections (ls_inks' and ls_nodes' Graph) deleting the record directly.
func (a *arangoDB) processEdgeRemoval(ctx context.Context, key string, action string) error {
	doc, err := a.graph.RemoveDocument(ctx, key)
	if err != nil {
		if !driver.IsNotFound(err) {
			return err
		}
		return nil
	}
	notifyKafka(doc, action)
	return nil
}

// processEdgeRemoval removes all documents where removed Vertex (ls_node) is referenced in "_to" or "_from"
func (a *arangoDB) processVertexRemoval(ctx context.Context, key string, action string) error {
	query := "FOR d IN " + a.graph.Name() +
		" filter d._to == " + "\"" + key + "\"" + " OR" + " d._from == " + "\"" + key + "\"" +
		" return d"
	cursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	for {
		var p lslinkEdgeObject
		_, err := cursor.ReadDocument(ctx, &p)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		glog.V(6).Infof("Removing from %s object %s", a.graph.Name(), p.Key)
		var doc driver.DocumentMeta
		if doc, err = a.graph.RemoveDocument(ctx, p.Key); err != nil {
			if !driver.IsNotFound(err) {
				return err
			}
			return nil
		}
		notifyKafka(doc, action)
	}
	return nil
}
