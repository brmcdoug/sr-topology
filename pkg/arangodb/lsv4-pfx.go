package arangodb

import (
	"context"
	"strconv"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/base"
	"github.com/sbezverk/gobmp/pkg/message"
)

//filters ls_prefixes matching ls_links out and passes remaining ls_prefixes to processLinkStatePrefix
func (a *arangoDB) processLSv4Pfx(ctx context.Context, key string, e *message.LSPrefix) error {
	glog.V(5).Infof("ls prefix: %s", e)
	if e.ProtocolID == base.BGP {
		// EPE Case cannot be processed because LS Node collection does not have BGP routers
		return nil
	}
	// lsv4pfx_remove := "for p in lsv4_pfx for l in ls_link filter p.prefix == l.local_link_ip remove p in lsv4_pfx"
	// ncursor, err := a.db.Query(ctx, lsv4pfx_remove, nil)
	// if err != nil {
	// 	return err
	// }

	query := "FOR d IN " + a.srnode.Name() +
		" filter d.igp_router_id == " + "\"" + e.IGPRouterID + "\"" +
		" filter d.domain_id == " + strconv.Itoa(int(e.DomainID)) +
		" filter d.protocol_id == " + strconv.Itoa(int(e.ProtocolID))
	query += " return d"
	glog.V(5).Infof("Query: %s", query)
	ncursor, err := a.db.Query(ctx, query, nil)

	if err != nil {
		return err
	}
	defer ncursor.Close()
	var nm SRNode
	mn, err := ncursor.ReadDocument(ctx, &nm)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}

	glog.V(6).Infof("node %s + prefix %s", nm.Key, e.Key)

	mtid := uint16(0)
	if e.MTID != nil {
		mtid = uint16(e.MTID.MTID)
	}
	ne := lsprefixEdgeObject{
		Key:                  key,
		From:                 e.ID,
		To:                   mn.ID.String(),
		PrefixKey:            e.Key,
		ProtocolID:           e.ProtocolID,
		DomainID:             e.DomainID,
		MTID:                 uint16(mtid),
		AreaID:               e.AreaID,
		IGPRouterID:          e.IGPRouterID,
		RouterID:             nm.RouterID,
		LocalNodeASN:         nm.ASN,
		Prefix:               e.Prefix,
		PrefixLen:            e.PrefixLen,
		PrefixMetric:         e.PrefixMetric,
		PrefixAttrTLVs:       e.PrefixAttrTLVs,
		FlexAlgoPrefixMetric: e.FlexAlgoPrefixMetric,
	}

	if _, err := a.graph.CreateDocument(ctx, &ne); err != nil {
		if !driver.IsConflict(err) {
			return err
		}
		// The document already exists, updating it with the latest info
		if _, err := a.graph.UpdateDocument(ctx, ne.Key, &ne); err != nil {
			return err
		}
	}

	return nil
}

// processLSRemoval removes a record from Node's graph collection
// since the key matches in both collections (ls_inks' and ls_nodes' Graph) deleting the record directly.
func (a *arangoDB) processLSPfxRemoval(ctx context.Context, key string, action string) error {
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
