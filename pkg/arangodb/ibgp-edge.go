package arangodb

import (
	"context"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

// processEdge processes a unicast prefix connection which is a unidirectional edge between an eBGP peer and LSNode
func (a *arangoDB) processIBGP(ctx context.Context, key string, e *SRNode) error {
	query := "for l in unicast_prefix_v4" +
		" filter l.peer_ip == " + "\"" + e.RouterID + "\"" + " filter l.origin_as == Null "
	query += " return l	"
	glog.V(5).Infof("running query: %s", query)
	pcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var up message.UnicastPrefix
		mp, err := pcursor.ReadDocument(ctx, &up)
		if err != nil {
			if driver.IsNoMoreDocuments(err) {
				return err
			}
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}

		glog.V(5).Infof("ls node %s + unicastprefix %s", e.Key, up.Key)
		ne := unicastPrefixEdgeObject{
			Key:       mp.ID.Key(),
			From:      e.ID,
			To:        mp.ID.String(),
			Prefix:    up.Prefix,
			PrefixLen: up.PrefixLen,
			LocalIP:   e.RouterID,
			PeerIP:    up.PeerIP,
			BaseAttrs: up.BaseAttributes,
			PeerASN:   e.PeerASN,
			OriginAS:  up.OriginAS,
			Nexthop:   up.Nexthop,
			Labels:    up.Labels,
			Name:      e.Name,
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
	}

	return nil
}

// processEdge processes a unicast prefix connection which is a unidirectional edge between an eBGP peer and LSNode
func (a *arangoDB) processInternalPrefix(ctx context.Context, key string, e *message.UnicastPrefix) error {
	query := "FOR d IN " + a.srnode.Name() +
		" filter d.router_id == " + "\"" + e.PeerIP + "\"" //+
	//" FOR l in ls_link " +
	//" filter d.remote_ip == l.remote_link_ip "
	query += " return d"
	glog.V(5).Infof("running filtered query: %s", query)
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

	glog.V(5).Infof("peer %s + prefix %s", nm.Key, e.Key)

	ne := unicastPrefixEdgeObject{
		Key:       key,
		From:      mn.ID.String(),
		To:        e.ID,
		Prefix:    e.Prefix,
		PrefixLen: e.PrefixLen,
		LocalIP:   e.RouterIP,
		PeerIP:    e.PeerIP,
		BaseAttrs: e.BaseAttributes,
		PeerASN:   e.PeerASN,
		OriginAS:  e.OriginAS,
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
