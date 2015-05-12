package mesos

import (
	"github.com/docker/docker/pkg/stringid"
	"github.com/docker/swarm/cluster"
	"github.com/gogo/protobuf/proto"
	"github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/mesosutil"
)

type task struct {
	mesosproto.TaskInfo

	updates chan *mesosproto.TaskStatus
}

func newTask(config *cluster.ContainerConfig, name, slaveID string) (*task, error) {
	task := task{
		updates: make(chan *mesosproto.TaskStatus),
	}

	ID := stringid.GenerateRandomID()

	resources := []*mesosproto.Resource{}

	if cpus := config.CpuShares; cpus > 0 {
		resources = append(resources, mesosutil.NewScalarResource("cpus", float64(cpus)))
	}

	if mem := config.Memory; mem > 0 {
		resources = append(resources, mesosutil.NewScalarResource("mem", float64(mem/1024/1024)))
	}

	taskInfo := mesosproto.TaskInfo{
		Name: &name,
		TaskId: &mesosproto.TaskID{
			Value: &ID,
		},
		SlaveId: &mesosproto.SlaveID{
			Value: &slaveID,
		},
		Resources: resources,
		Command:   &mesosproto.CommandInfo{},
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

	task.TaskInfo = taskInfo
	return &task, nil
}

func (t *task) sendStatus(status *mesosproto.TaskStatus) {
	t.updates <- status
}

func (t *task) getStatus() *mesosproto.TaskStatus {
	return <-t.updates
}
