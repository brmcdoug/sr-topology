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
func (a *arangoDB) processLSPrefix(ctx context.Context, key string, e *message.LSPrefix) error {
	if e.ProtocolID == base.BGP {
		// EPE Case cannot be processed because LS Node collection does not have BGP routers
		return nil
	}
	query := "FOR l IN " + a.srnode.Name() +
		" filter l.igp_router_id == " + "\"" + e.IGPRouterID + "\"" +
		" filter l.domain_id == " + strconv.Itoa(int(e.DomainID)) +
		" filter l.protocol_id == " + strconv.Itoa(int(e.ProtocolID)) +
		" filter l.prefix_len != 30 && l.prefix_len != 31 && l.prefix_len != 32 && l.prefix_metric == null"
	query += " return l"
	//glog.V(5).Infof("Query: %s", query)
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
func (a *arangoDB) processLSPrefixRemoval(ctx context.Context, key string, action string) error {
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
