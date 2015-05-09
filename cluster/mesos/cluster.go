package mesos

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/swarm/cluster"
	"github.com/docker/swarm/scheduler"
	"github.com/docker/swarm/scheduler/node"
	"github.com/docker/swarm/state"
	"github.com/mesos/mesos-go/mesosproto"
	mesosscheduler "github.com/mesos/mesos-go/scheduler"
	"github.com/samalba/dockerclient"
)

// Cluster struct for mesos
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
	frameworkName       = "swarm"
	dockerDaemonPort    = "2375"
	defaultOfferTimeout = "10m"
	errNotSupported     = errors.New("not supported with mesos")
)

// NewCluster for mesos Cluster creation
func NewCluster(scheduler *scheduler.Scheduler, store *state.Store, options *cluster.Options) cluster.Cluster {
	log.WithFields(log.Fields{"name": "mesos"}).Debug("Initializing cluster")

	cluster := &Cluster{
		slaves:    make(map[string]*slave),
		scheduler: scheduler,
		options:   options,
		store:     store,
	}

	// Empty string is accepted by the scheduler.
	user := os.Getenv("SWARM_MESOS_USER")

	driverConfig := mesosscheduler.DriverConfig{
		Scheduler: cluster,
		Framework: &mesosproto.FrameworkInfo{Name: &frameworkName, User: &user},
		Master:    options.Discovery,
	}

	// Changing port for https
	if options.TLSConfig != nil {
		dockerDaemonPort = "2376"
	}

	bindingAddressEnv := os.Getenv("SWARM_MESOS_ADDRESS")
	bindingPortEnv := os.Getenv("SWARM_MESOS_PORT")

	if bindingPortEnv != "" {
		log.Debugf("SWARM_MESOS_PORT found, Binding port to %s", bindingPortEnv)
		bindingPort, err := strconv.ParseUint(bindingPortEnv, 0, 16)
		if err != nil {
			log.Errorf("Unable to parse SWARM_MESOS_PORT, error: %s", err)
			return nil
		}
		driverConfig.BindingPort = uint16(bindingPort)
	}

	if bindingAddressEnv != "" {
		log.Debugf("SWARM_MESOS_ADDRESS found, Binding address to %s", bindingAddressEnv)
		bindingAddress := net.ParseIP(bindingAddressEnv)
		if bindingAddress == nil {
			log.Error("Unable to parse SWARM_MESOS_ADDRESS")
			return nil
		}
		driverConfig.BindingAddress = bindingAddress
	}

	driver, err := mesosscheduler.NewMesosSchedulerDriver(driverConfig)
	if err != nil {
		return nil
	}

	cluster.driver = driver

	status, err := driver.Start()
	if err != nil {
		log.Debugf("Mesos driver started, status/err %v: %v", status, err)
		return nil
	}
	log.Debugf("Mesos driver started, status %v", status)

	return cluster
}

// RegisterEventHandler registers an event handler.
func (c *Cluster) RegisterEventHandler(h cluster.EventHandler) error {
	if c.eventHandler != nil {
		return errors.New("event handler already set")
	}
	c.eventHandler = h
	return nil
}

// CreateContainer for container creation in Mesos task
func (c *Cluster) CreateContainer(config *cluster.ContainerConfig, name string) (*cluster.Container, error) {

	n, err := c.scheduler.SelectNodeForContainer(c.listNodes(), config)
	if err != nil {
		return nil, err
	}

	task, err := newTask(config, name, n.ID)
	if err != nil {
		return nil, err
	}

	s, ok := c.slaves[n.ID]
	if !ok {
		return nil, nil
	}

	s.Lock()
	// TODO: Only use the offer we need
	offerIds := []*mesosproto.OfferID{}
	for _, offer := range c.slaves[n.ID].offers {
		offerIds = append(offerIds, offer.Id)
	}

	if _, err := c.driver.LaunchTasks(offerIds, []*mesosproto.TaskInfo{&task.TaskInfo}, &mesosproto.Filters{}); err != nil {
		s.Unlock()
		return nil, err
	}

	s.addTask(task)

	// TODO: Do not erase all the offers, only the one used
	for _, offer := range s.offers {
		c.removeOffer(offer)
	}
	s.Unlock()
	// block until we get the container
	finished, err := c.monitorTask(task)

	if err != nil {
		//remove task
		s.removeTask(task.TaskInfo.TaskId.GetValue())
		return nil, err
	}
	if !finished {
		go func() {
			for {
				finished, err := c.monitorTask(task)
				if err != nil {
					// TODO proper error message
					log.Error(err)
					break
				}
				if finished {
					break
				}
			}
			//remove task
		}()
	}

	// Register the container immediately while waiting for a state refresh.
	// Force a state refresh to pick up the newly created container.
	s.engine.RefreshContainers(true)

	// TODO: We have to return the right container that was just created.
	// Once we receive the ContainerID from the executor.
	for _, container := range s.engine.Containers() {
		return container, nil
	}

	return nil, fmt.Errorf("Container failed to create")

	// TODO: do not store the container as it might be a wrong ContainerID
	// see TODO in slave.go
	//st := &state.RequestedState{
	//ID:     container.Id,
	//Name:   name,
	//Config: config,
	//}
	//return container, nil //c.store.Add(container.Id, st)
}

func (c *Cluster) monitorTask(task *task) (bool, error) {
	taskStatus := task.getStatus()

	switch taskStatus.GetState() {
	case mesosproto.TaskState_TASK_STAGING:
	case mesosproto.TaskState_TASK_STARTING:
	case mesosproto.TaskState_TASK_RUNNING:
	case mesosproto.TaskState_TASK_FINISHED:
		return true, nil
	case mesosproto.TaskState_TASK_FAILED:
		return true, errors.New(taskStatus.GetMessage())
	case mesosproto.TaskState_TASK_KILLED:
		return true, nil
	case mesosproto.TaskState_TASK_LOST:
		return true, errors.New(taskStatus.GetMessage())
	case mesosproto.TaskState_TASK_ERROR:
		return true, errors.New(taskStatus.GetMessage())
	}

	return false, nil
}

// RemoveContainer to remove containers on mesos cluster
func (c *Cluster) RemoveContainer(container *cluster.Container, force bool) error {
	return nil
}

// Images returns all the images in the cluster.
func (c *Cluster) Images() []*cluster.Image {
	c.RLock()
	defer c.RUnlock()

	out := []*cluster.Image{}
	for _, s := range c.slaves {
		out = append(out, s.engine.Images()...)
	}

	return out
}

// Image returns an image with IdOrName in the cluster
func (c *Cluster) Image(IDOrName string) *cluster.Image {
	// Abort immediately if the name is empty.
	if len(IDOrName) == 0 {
		return nil
	}

	c.RLock()
	defer c.RUnlock()
	for _, s := range c.slaves {
		if image := s.engine.Image(IDOrName); image != nil {
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
	for _, s := range c.slaves {
		out = append(out, s.engine.Containers()...)
	}

	return out
}

// Container returns the container with IdOrName in the cluster
func (c *Cluster) Container(IDOrName string) *cluster.Container {
	// Abort immediately if the name is empty.
	if len(IDOrName) == 0 {
		return nil
	}

	c.RLock()
	defer c.RUnlock()
	for _, s := range c.slaves {
		if container := s.engine.Container(IDOrName); container != nil {
			return container
		}
	}

	return nil
}

// RemoveImage removes an image from the cluster
func (c *Cluster) RemoveImage(image *cluster.Image) ([]*dockerclient.ImageDelete, error) {
	return nil, nil
}

// Pull will pull images on the cluster nodes
func (c *Cluster) Pull(name string, authConfig *dockerclient.AuthConfig, callback func(what, status string)) {

}

// Load images
func (c *Cluster) Load(imageReader io.Reader, callback func(what, status string)) {

}

// RenameContainer Rename a container
func (c *Cluster) RenameContainer(container *cluster.Container, newName string) error {
	return errNotSupported
}

func scalarResourceValue(offers map[string]*mesosproto.Offer, name string) float64 {
	var value float64
	for _, offer := range offers {
		for _, resource := range offer.Resources {
			if *resource.Name == name {
				value += *resource.Scalar.Value
			}
		}
	}
	return value
}

// listNodes returns all the nodess in the cluster.
func (c *Cluster) listNodes() []*node.Node {
	c.RLock()
	defer c.RUnlock()

	out := []*node.Node{}
	for _, s := range c.slaves {
		n := node.NewNode(s.engine)
		n.ID = s.id
		n.TotalCpus = int64(scalarResourceValue(s.offers, "cpus"))
		n.UsedCpus = 0
		n.TotalMemory = int64(scalarResourceValue(s.offers, "mem")) * 1024 * 1024
		n.UsedMemory = 0
		out = append(out, n)
	}
	return out
}

func (c *Cluster) listOffers() []*mesosproto.Offer {
	list := []*mesosproto.Offer{}
	for _, s := range c.slaves {
		for _, offer := range s.offers {
			list = append(list, offer)
		}
	}
	return list
}

// Info gives minimal information about containers and resources on the mesos cluster
func (c *Cluster) Info() [][2]string {
	offers := c.listOffers()
	info := [][2]string{
		{"\bStrategy", c.scheduler.Strategy()},
		{"\bFilters", c.scheduler.Filters()},
		{"\bOffers", fmt.Sprintf("%d", len(offers))},
	}

	sort.Sort(offerSorter(offers))

	for _, offer := range offers {
		info = append(info, [2]string{" Offer", offer.Id.GetValue()})
		for _, resource := range offer.Resources {
			info = append(info, [2]string{"  └ " + *resource.Name, fmt.Sprintf("%v", resource)})
		}
	}

	return info
}

// Registered method for registered mesos framework
func (c *Cluster) Registered(driver mesosscheduler.SchedulerDriver, fwID *mesosproto.FrameworkID, masterInfo *mesosproto.MasterInfo) {
	log.WithFields(log.Fields{"name": "mesos", "frameworkId": fwID.GetValue()}).Debug("Framework registered")
}

// Reregistered method for registered mesos framework
func (c *Cluster) Reregistered(mesosscheduler.SchedulerDriver, *mesosproto.MasterInfo) {
	log.WithFields(log.Fields{"name": "mesos"}).Debug("Framework re-registered")
}

// Disconnected method
func (c *Cluster) Disconnected(mesosscheduler.SchedulerDriver) {
	log.WithFields(log.Fields{"name": "mesos"}).Debug("Framework disconnected")
}

// ResourceOffers method
func (c *Cluster) ResourceOffers(_ mesosscheduler.SchedulerDriver, offers []*mesosproto.Offer) {
	log.WithFields(log.Fields{"name": "mesos", "offers": len(offers)}).Debug("Offers received")

	for _, offer := range offers {
		slaveID := offer.SlaveId.GetValue()
		s, ok := c.slaves[slaveID]
		if !ok {
			engine := cluster.NewEngine(*offer.Hostname+":"+dockerDaemonPort, 0)
			if err := engine.Connect(c.options.TLSConfig); err != nil {
				log.Error(err)
			} else {
				s = newSlave(slaveID, engine)
				c.slaves[slaveID] = s
			}
		}
		s.addOffer(offer)
	}
}

func (c *Cluster) addOffer(offer *mesosproto.Offer) {
	s, ok := c.slaves[offer.SlaveId.GetValue()]
	if !ok {
		return
	}
	s.addOffer(offer)
	go func(offer *mesosproto.Offer) {
		offerTimeout := os.Getenv("SWARM_MESOS_OFFER_TIMEOUT")
		if offerTimeout == "" {
			offerTimeout = defaultOfferTimeout
		}

		d, err := time.ParseDuration(offerTimeout)
		if err != nil {
			d = 10 * time.Minute
		}
		<-time.After(d)
		if c.removeOffer(offer) {
			if _, err := c.driver.DeclineOffer(offer.Id, &mesosproto.Filters{}); err != nil {
				log.WithFields(log.Fields{"name": "mesos"}).Errorf("Error while declining offer %q: %v", offer.Id.GetValue(), err)
			} else {
				log.WithFields(log.Fields{"name": "mesos"}).Debugf("Offer %q declined successfully", offer.Id.GetValue())
			}
		}
	}(offer)
}

func (c *Cluster) removeOffer(offer *mesosproto.Offer) bool {
	log.WithFields(log.Fields{"name": "mesos", "offerID": offer.Id.String()}).Debug("Removing offer")
	s, ok := c.slaves[offer.SlaveId.GetValue()]
	if !ok {
		return false
	}
	found := s.removeOffer(offer.Id.GetValue())
	if s.empty() {
		// Disconnect from engine
		delete(c.slaves, offer.SlaveId.GetValue())
	}
	return found
}

// OfferRescinded method
func (c *Cluster) OfferRescinded(mesosscheduler.SchedulerDriver, *mesosproto.OfferID) {
}

// StatusUpdate method
func (c *Cluster) StatusUpdate(_ mesosscheduler.SchedulerDriver, taskStatus *mesosproto.TaskStatus) {
	log.WithFields(log.Fields{"name": "mesos", "state": taskStatus.State.String()}).Debug("Status update")
	taskID := taskStatus.TaskId.GetValue()
	slaveID := taskStatus.SlaveId.GetValue()
	s, ok := c.slaves[slaveID]
	if !ok {
		return
	}
	if task, ok := s.tasks[taskID]; ok {
		task.sendStatus(taskStatus)
	} else {
		var reason = ""
		if taskStatus.Reason != nil {
			reason = taskStatus.GetReason().String()
		}

		log.WithFields(log.Fields{
			"name":    "mesos",
			"state":   taskStatus.State.String(),
			"slaveId": taskStatus.SlaveId.GetValue(),
			"reason":  reason,
		}).Warn("Status update received for unknown slave")
	}
}

// FrameworkMessage method
func (c *Cluster) FrameworkMessage(mesosscheduler.SchedulerDriver, *mesosproto.ExecutorID, *mesosproto.SlaveID, string) {
}

// SlaveLost method
func (c *Cluster) SlaveLost(mesosscheduler.SchedulerDriver, *mesosproto.SlaveID) {
}

// ExecutorLost method
func (c *Cluster) ExecutorLost(mesosscheduler.SchedulerDriver, *mesosproto.ExecutorID, *mesosproto.SlaveID, int) {
}

// Error method
func (c *Cluster) Error(d mesosscheduler.SchedulerDriver, msg string) {
	log.WithFields(log.Fields{"name": "mesos"}).Error(msg)
}

// RANDOMENGINE returns a random engine.
func (c *Cluster) RANDOMENGINE() (*cluster.Engine, error) {
	n, err := c.scheduler.SelectNodeForContainer(c.listNodes(), &cluster.ContainerConfig{})
	if err != nil {
		return nil, err
	}
	if n != nil {
		return c.slaves[n.ID].engine, nil
	}
	return nil, nil
}
