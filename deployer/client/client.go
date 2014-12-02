package client

import (
	"errors"
	"net/http"

	"github.com/flynn/flynn/deployer/types"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/discoverd/client/dialer"
	"github.com/flynn/flynn/pkg/httpclient"
)

// ErrNotFound is returned when no job was found.
var ErrNotFound = errors.New("deployer: job not found")

type Client struct {
	*httpclient.Client
}

// New uses the default discoverd client and returns a client.
func New() (*Client, error) {
	if err := discoverd.Connect(""); err != nil {
		return nil, err
	}
	dialer := dialer.New(discoverd.DefaultClient, nil)
	c := &httpclient.Client{
		Dial:        dialer.Dial,
		DialClose:   dialer,
		ErrPrefix:   "deployer",
		ErrNotFound: ErrNotFound,
		URL:         "http://flynn-deployer",
	}
	c.HTTP = &http.Client{Transport: &http.Transport{Dial: c.Dial}}
	return &Client{Client: c}, nil
}

func (c *Client) QueueJob(job *deployer.Job) error {
	return c.Post("/jobs", job, job)
}

func (c *Client) GetJob(id string) (*deployer.Job, error) {
	job := &deployer.Job{}
	err := c.Get("/jobs/"+id, job)
	return job, err
}
