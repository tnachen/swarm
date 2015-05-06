package mesos

import (
	"fmt"
	"sync"

	"github.com/docker/swarm/cluster"
	"github.com/samalba/dockerclient"
)

type createRequest struct {
	ID        string
	config    *dockerclient.ContainerConfig
	name      string
	error     chan error
	container chan *cluster.Container
}

type containerQueue struct {
	sync.RWMutex
	crs []*createRequest
	c   *Cluster
}

func (crs *containerQueue) scheduleContainer(cr *createRequest) bool {
	n, err := crs.c.scheduler.SelectNodeForContainer(crs.c.listNodes(), cr.config)
	if err != nil {
		return false
	}

	if nn, ok := crs.c.slaves[n.ID]; ok {
		container, err := nn.create(cr.ID, crs.c.driver, cr.config, cr.name, true)
		if err != nil {
			cr.error <- err
			return true
		}
		if container == nil {
			cr.error <- fmt.Errorf("Container failed to create")
			return true
		}

		// TODO: do not store the container as it might be a wrong ContainerID
		// see TODO in slave.go
		//st := &state.RequestedState{
		//ID:     container.Id,
		//Name:   name,
		//Config: config,
		//}
		cr.container <- container
		return true
	}
	cr.error <- fmt.Errorf("Unable to create on slave %q", n.ID)
	return true
}

func (crs *containerQueue) add(cr *createRequest) {
	crs.Lock()
	defer crs.Unlock()

	if !crs.scheduleContainer(cr) {
		crs.crs = append(crs.crs, cr)
	}
}

func (crs *containerQueue) remove(lock bool, taskIDs ...string) {
	if lock {
		crs.Lock()
		defer crs.Unlock()
	}

	new := []*createRequest{}
	for _, cr := range crs.crs {
		found := false
		for _, taskID := range taskIDs {
			if cr.ID == taskID {
				found = true
			}
		}
		if !found {
			new = append(new, cr)
		}
	}
	crs.crs = new
}

func (crs *containerQueue) resourcesAdded() {
	go crs.process()
}

func (crs *containerQueue) process() {
	crs.Lock()
	defer crs.Unlock()

	ids := []string{}
	for _, cr := range crs.crs {
		if crs.scheduleContainer(cr) {
			ids = append(ids, cr.ID)
		}
	}

	crs.remove(false, ids...)
}
