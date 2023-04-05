package arangodb

import (
	"context"
	"fmt"
	"os"
	"strings"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	notifier "github.com/jalapeno/topology/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/message"
	"wwwin-github.cisco.com/brmcdoug/sr-topology/pkg/kafkanotifier"
)

const srTopologyCollection = "sr_topology"

var Notifier *kafkanotifier.Notifier

func InitializeKafkaNotifier(msgSrvAddr string) {
	kNotifier, err := kafkanotifier.NewKafkaNotifier(msgSrvAddr)
	if err != nil {
		glog.Errorf("failed to initialize events notifier with error: %+v", err)
		os.Exit(1)
	}
	Notifier = kNotifier
}

func (a *arangoDB) lsLinkHandler(obj *notifier.EventMessage) error {
	ctx := context.TODO()
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}
	// Check if Collection encoded in ID exists
	c := strings.Split(obj.ID, "/")[0]
	if strings.Compare(c, a.lslink.Name()) != 0 {
		return fmt.Errorf("configured collection name %s and received in event collection name %s do not match", a.lslink.Name(), c)
	}
	glog.V(5).Infof("Processing action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.LSLink
	_, err := a.lslink.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a LSLink removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "del" then it is confirmed delete operation, otherwise return error
		if obj.Action != "del" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		return a.processEdgeRemoval(ctx, obj.Key, obj.Action)
	}
	switch obj.Action {
	case "add":
		fallthrough
	case "update":
		if err := a.processEdge(ctx, obj.Key, &o); err != nil {
			return fmt.Errorf("failed to process action %s for edge %s with error: %+v", obj.Action, obj.Key, err)
		}
	}
	//glog.V(5).Infof("Processing action: %s", obj.Action, obj.ID)
	return nil
}

func (a *arangoDB) lsPrefixHandler(obj *notifier.EventMessage) error {
	ctx := context.TODO()
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}
	// Check if Collection encoded in ID exists
	c := strings.Split(obj.ID, "/")[0]
	if strings.Compare(c, a.lsprefix.Name()) != 0 {
		return fmt.Errorf("configured collection name %s and received in event collection name %s do not match", a.lsprefix.Name(), c)
	}
	glog.V(5).Infof("Processing action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.LSPrefix
	_, err := a.lsprefix.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a LS prefix removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "del" then it is confirmed delete operation, otherwise return error
		if obj.Action != "del" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		return a.processLSPrefixRemoval(ctx, obj.Key, obj.Action)
	}
	switch obj.Action {
	case "add":
		fallthrough
	case "update":
		if err := a.processLSPrefix(ctx, obj.Key, &o); err != nil {
			return fmt.Errorf("failed to process action %s for edge %s with error: %+v", obj.Action, obj.Key, err)
		}
	}
	glog.V(5).Infof("Processing action: %s", obj.Action, obj.ID)
	return nil
}

func (a *arangoDB) lsv4PfxHandler(obj *notifier.EventMessage) error {
	ctx := context.TODO()
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}
	// Check if Collection encoded in ID exists
	c := strings.Split(obj.ID, "/")[0]
	if strings.Compare(c, a.lsprefix.Name()) != 0 {
		return fmt.Errorf("configured collection name %s and received in event collection name %s do not match", a.lsprefix.Name(), c)
	}
	glog.V(5).Infof("Processing action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.LSPrefix
	_, err := a.lsprefix.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a LS prefix removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "del" then it is confirmed delete operation, otherwise return error
		if obj.Action != "del" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		return a.processLSPrefixRemoval(ctx, obj.Key, obj.Action)
	}
	switch obj.Action {
	case "add":
		fallthrough
	case "update":
		if err := a.processLSv4Pfx(ctx, obj.Key, &o); err != nil {
			return fmt.Errorf("failed to process action %s for edge %s with error: %+v", obj.Action, obj.Key, err)
		}
	}
	glog.V(5).Infof("Processing action: %s", obj.Action, obj.ID)
	return nil
}

func (a *arangoDB) unicastPrefixHandler(obj *notifier.EventMessage) error {
	ctx := context.TODO()
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}
	// todo: unicast prefix edge removal not working properly (january 20, 2022)

	glog.V(5).Infof("Processing action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.UnicastPrefix
	_, err := a.unicastprefixv4.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a UnicastPrefix removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "del" then it is confirmed delete operation, otherwise return error
		if obj.Action != "del" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		return a.processUnicastPrefixRemoval(ctx, obj.ID)
	}
	switch obj.Action {
	case "update":
		glog.V(5).Infof("Send update msg to processEPEPrefix function")
		if err := a.processEPEPrefix(ctx, obj.Key, &o); err != nil {
			return fmt.Errorf("failed to process action %s for edge %s with error: %+v", obj.Action, obj.Key, err)
		}
	case "add":
		glog.V(5).Infof("Send add msg to processEPEPrefix function")
		if err := a.processEPEPrefix(ctx, obj.Key, &o); err != nil {
			return fmt.Errorf("failed to process action %s for edge %s with error: %+v", obj.Action, obj.Key, err)
		}
	default:
		// NOOP
	}

	return nil
}

func (a *arangoDB) PeerHandler(obj *notifier.EventMessage) error {
	ctx := context.TODO()
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}
	// Check if Collection encoded in ID exists
	c := strings.Split(obj.ID, "/")[0]
	if strings.Compare(c, a.peer.Name()) != 0 {
		return fmt.Errorf("configured collection name %s and received in event collection name %s do not match", a.peer.Name(), c)
	}
	//glog.V(5).Infof("Processing action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.PeerStateChange
	_, err := a.peer.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a BGP Peer removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "del" then it is confirmed delete operation, otherwise return error
		if obj.Action != "del" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		return a.processPeerRemoval(ctx, obj.ID)
	}
	switch obj.Action {
	case "add":
		if err := a.processEdgeByPeer(ctx, obj.Key, &o); err != nil {
			return fmt.Errorf("failed to process action %s for vertex %s with error: %+v", obj.Action, obj.Key, err)
		}
	default:
		// NOOP for update
	}

	return nil
}

func notifyKafka(doc driver.DocumentMeta, action string) {
	kafkanotifier.TriggerNotification(Notifier, kafkanotifier.LinkStateEdgeTopic, &notifier.EventMessage{
		Key:    doc.Key,
		ID:     srTopologyCollection + "/" + doc.Key,
		Action: action,
	})
}
