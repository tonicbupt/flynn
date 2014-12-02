package main

import (
	"fmt"

	c "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	deployerc "github.com/flynn/flynn/deployer/client"
	"github.com/flynn/flynn/deployer/types"
)

type DeployerSuite struct {
	Helper
}

var _ = c.Suite(&DeployerSuite{})

func (s *DeployerSuite) TearDownSuite(t *c.C) {
	s.cleanup()
}

func (s *DeployerSuite) TestJobQueue(t *c.C) {
	client, err := deployerc.New()
	t.Assert(err, c.IsNil)
	job := &deployer.Job{
		Steps: []deployer.Step{
			{ReleaseID: "foo", Cmd: []string{"a", "b", "c"}},
			{ReleaseID: "bar", Cmd: []string{"d", "e", "f"}},
		},
	}
	t.Assert(client.QueueJob(job), c.IsNil)

	j, err := client.GetJob(job.ID)
	fmt.Printf("JOB: %+v\n", j)
	t.Assert(err, c.IsNil)
	t.Assert(j.ID, c.Equals, job.ID)
	t.Assert(j.Steps, c.DeepEquals, job.Steps)
}
