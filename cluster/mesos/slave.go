package mesos

import (
	"crypto/rand"
	"encoding/hex"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/swarm/cluster"
	"github.com/gogo/protobuf/proto"
	"github.com/mesos/mesos-go/mesosproto"
	mesosscheduler "github.com/mesos/mesos-go/scheduler"
	"github.com/samalba/dockerclient"
)

type slave struct {
	cluster.Engine

	offers  []*mesosproto.Offer
	updates map[string]chan string
}

func NewSlave(addr string, overcommitRatio float64, offer *mesosproto.Offer) *slave {
	slave := &slave{Engine: *cluster.NewEngine(addr, overcommitRatio)}
	slave.offers = []*mesosproto.Offer{offer}
	slave.updates = make(map[string]chan string)
	return slave
}

func (n *slave) addOffer(offer *mesosproto.Offer) {
	n.offers = append(n.offers, offer)
}

func (n *slave) create(driver *mesosscheduler.MesosSchedulerDriver, config *dockerclient.ContainerConfig, name string, pullImage bool) (*cluster.Container, error) {

	id := make([]byte, 6)
	nn, err := rand.Read(id)
	if nn != len(id) || err != nil {
		return nil, err
	}
	ID := hex.EncodeToString(id)

	n.updates[ID] = make(chan string)

	cpus := "cpus"
	typ := mesosproto.Value_SCALAR
	val := 1.0

	taskInfo := &mesosproto.TaskInfo{
		Name: &name,
		TaskId: &mesosproto.TaskID{
			Value: &ID,
		},
		SlaveId: n.offers[0].SlaveId,
		Resources: []*mesosproto.Resource{
			{
				Name: &cpus,
				Type: &typ,
				Scalar: &mesosproto.Value_Scalar{
					Value: &val,
				},
			},
		},
		Command: &mesosproto.CommandInfo{},
	}

	if len(config.Cmd) > 0 && config.Cmd[0] != "" {
		taskInfo.Command.Value = &config.Cmd[0]
	}

	if len(config.Cmd) > 1 {
		taskInfo.Command.Arguments = config.Cmd[1:]
	}

	taskInfo.Container = &mesosproto.ContainerInfo{
		Type: mesosproto.ContainerInfo_DOCKER.Enum(),
		Docker: &mesosproto.ContainerInfo_DockerInfo{
			Image: &config.Image,
		},
	}

	taskInfo.Command.Shell = proto.Bool(false)

	offerIds := []*mesosproto.OfferID{}

	for _, offer := range n.offers {
		offerIds = append(offerIds, offer.Id)
	}

	status, err := driver.LaunchTasks(offerIds, []*mesosproto.TaskInfo{taskInfo}, &mesosproto.Filters{})
	if err != nil {
		return nil, err
	}
	log.Debugf("create %v: %v", status, err)

	n.offers = []*mesosproto.Offer{}

	// block until we get the container
	<-n.updates[ID]

	// Register the container immediately while waiting for a state refresh.
	// Force a state refresh to pick up the newly created container.
	n.RefreshContainers(true)

	n.RLock()
	defer n.RUnlock()

	// TODO: We have to return the right container that was just created.
	// Once we receive the ContainerID from the executor.
	for _, container := range n.Containers() {
		return container, nil
	}

	return nil, nil
}
