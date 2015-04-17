package mesos

import (
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/swarm/cluster"
	"github.com/docker/swarm/scheduler"
	"github.com/docker/swarm/scheduler/node"
	"github.com/docker/swarm/state"
	"github.com/mesos/mesos-go/mesosproto"
	mesosscheduler "github.com/mesos/mesos-go/scheduler"
	"github.com/samalba/dockerclient"
)

type Cluster struct {
	sync.RWMutex

	driver *mesosscheduler.MesosSchedulerDriver

	eventHandler cluster.EventHandler
	slaves       map[string]*slave
	scheduler    *scheduler.Scheduler
	options      *cluster.Options
	store        *state.Store
}

var (
	frameworkName = "swarm"
	user          = ""
)

func NewCluster(scheduler *scheduler.Scheduler, store *state.Store, eventhandler cluster.EventHandler, options *cluster.Options) cluster.Cluster {
	log.WithFields(log.Fields{"name": "mesos"}).Debug("Initializing cluster")

	cluster := &Cluster{
		eventHandler: eventhandler,
		slaves:       make(map[string]*slave),
		scheduler:    scheduler,
		options:      options,
		store:        store,
	}

	driverConfig := mesosscheduler.DriverConfig{
		Scheduler: cluster,
		Framework: &mesosproto.FrameworkInfo{Name: &frameworkName, User: &user},
		Master:    options.Discovery,
	}

	driver, err := mesosscheduler.NewMesosSchedulerDriver(driverConfig)
	if err != nil {
		return nil
	}

	cluster.driver = driver

	status, err := driver.Start()
	log.Debugf("NewCluster %v: %v", status, err)
	if err != nil {
		return nil
	}

	return cluster
}

func (c *Cluster) CreateContainer(config *dockerclient.ContainerConfig, name string) (*cluster.Container, error) {

	n, err := c.scheduler.SelectNodeForContainer(c.listNodes(), config)
	if err != nil {
		return nil, err
	}

	if nn, ok := c.slaves[n.ID]; ok {
		container, err := nn.create(c.driver, config, name, true)
		if container == nil {
			return nil, fmt.Errorf("Container failed to create")
		}

		if err != nil {
			return nil, err
		}

		st := &state.RequestedState{
			ID:     container.Id,
			Name:   name,
			Config: config,
		}
		return container, c.store.Add(container.Id, st)
	}
	return nil, nil
}

func (c *Cluster) RemoveContainer(container *cluster.Container, force bool) error {
	return nil
}

// Containers returns all the images in the cluster.
func (c *Cluster) Images() []*cluster.Image {
	c.RLock()
	defer c.RUnlock()

	out := []*cluster.Image{}
	for _, n := range c.slaves {
		out = append(out, n.Images()...)
	}

	return out
}

// Image returns an image with IdOrName in the cluster
func (c *Cluster) Image(IdOrName string) *cluster.Image {
	// Abort immediately if the name is empty.
	if len(IdOrName) == 0 {
		return nil
	}

	c.RLock()
	defer c.RUnlock()
	for _, n := range c.slaves {
		if image := n.Image(IdOrName); image != nil {
			return image
		}
	}

	return nil
}

// Containers returns all the containers in the cluster.
func (c *Cluster) Containers() []*cluster.Container {
	c.RLock()
	defer c.RUnlock()

	out := []*cluster.Container{}
	for _, n := range c.slaves {
		out = append(out, n.Containers()...)
	}

	return out
}

// Container returns the container with IdOrName in the cluster
func (c *Cluster) Container(IdOrName string) *cluster.Container {
	// Abort immediately if the name is empty.
	if len(IdOrName) == 0 {
		return nil
	}

	c.RLock()
	defer c.RUnlock()
	for _, n := range c.slaves {
		if container := n.Container(IdOrName); container != nil {
			return container
		}
	}

	return nil
}

func (c *Cluster) RemoveImage(image *cluster.Image) ([]*dockerclient.ImageDelete, error) {
	return nil, nil
}

func (c *Cluster) Pull(name string, callback func(what, status string)) {

}

// listNodes returns all the nodess in the cluster.
func (c *Cluster) listNodes() []*node.Node {
	c.RLock()
	defer c.RUnlock()

	out := []*node.Node{}
	for _, n := range c.slaves {
		out = append(out, node.NewNode(&n.Engine))
	}

	return out
}

// listEngines returns all the engines in the cluster.
func (c *Cluster) listEngines() []*slave {
	c.RLock()
	defer c.RUnlock()

	out := []*slave{}
	for _, s := range c.slaves {
		out = append(out, s)
	}
	return out
}

func (c *Cluster) Info() [][2]string {
	info := [][2]string{
		{"\bStrategy", c.scheduler.Strategy()},
		{"\bFilters", c.scheduler.Filters()},
		{"\bSlaves", fmt.Sprintf("%d", len(c.slaves))},
	}

	//sort.Sort(cluster.EngineSorter(nodes))

	for _, node := range c.listEngines() {
		info = append(info, [2]string{node.Name, node.Addr})
		for _, offer := range node.offers {
			info = append(info, [2]string{" Offer", offer.Id.GetValue()})
			for _, resource := range offer.Resources {
				info = append(info, [2]string{"  └ " + *resource.Name, fmt.Sprintf("%v", resource)})
			}
		}
	}

	return info
}

func (c *Cluster) Registered(mesosscheduler.SchedulerDriver, *mesosproto.FrameworkID, *mesosproto.MasterInfo) {
}

func (c *Cluster) Reregistered(mesosscheduler.SchedulerDriver, *mesosproto.MasterInfo) {
}

func (c *Cluster) Disconnected(mesosscheduler.SchedulerDriver) {
}

func (c *Cluster) ResourceOffers(_ mesosscheduler.SchedulerDriver, offers []*mesosproto.Offer) {
	log.WithFields(log.Fields{"name": "mesos", "offers": len(offers)}).Debug("Offers received")

	for _, offer := range offers {
		slaveId := offer.SlaveId.GetValue()
		if slave, ok := c.slaves[slaveId]; ok {
			slave.addOffer(offer)
		} else {
			slave := NewSlave(*offer.Hostname+":4242", c.options.OvercommitRatio, offer)
			err := slave.Connect(c.options.TLSConfig)
			if err != nil {
				log.Error(err)
			} else {
				c.slaves[slaveId] = slave
			}
		}
	}
}

func (c *Cluster) OfferRescinded(mesosscheduler.SchedulerDriver, *mesosproto.OfferID) {
}

func (c *Cluster) StatusUpdate(_ mesosscheduler.SchedulerDriver, taskStatus *mesosproto.TaskStatus) {
	log.WithFields(log.Fields{"name": "mesos", "state": taskStatus.State.String()}).Debug("Status update")

	ID := taskStatus.TaskId.GetValue()
	slaveId := taskStatus.SlaveId.GetValue()

	if slave, ok := c.slaves[slaveId]; ok {
		fmt.Println("Slave", slaveId, "found")
		slave.updates[ID] <- taskStatus.State.String()
	} else {
		fmt.Println("Slave", slaveId, "not found")
	}
	fmt.Println("end")
}

func (c *Cluster) FrameworkMessage(mesosscheduler.SchedulerDriver, *mesosproto.ExecutorID, *mesosproto.SlaveID, string) {
}

func (c *Cluster) SlaveLost(mesosscheduler.SchedulerDriver, *mesosproto.SlaveID) {
}

func (c *Cluster) ExecutorLost(mesosscheduler.SchedulerDriver, *mesosproto.ExecutorID, *mesosproto.SlaveID, int) {
}

func (c *Cluster) Error(d mesosscheduler.SchedulerDriver, msg string) {
	log.Error(msg)
}
