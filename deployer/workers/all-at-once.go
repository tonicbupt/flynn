package main

import (
	"github.com/flynn/flynn/controller/client"
	ct "github.com/flynn/flynn/controller/types"
)

type AllAtOnceDeployer struct {
	client *controller.Client
}

func (d *AllAtOnceDeployer) Deploy(appID, releaseID, prevReleaseID string) error {
	err := d.client.SetAppRelease(appID, releaseID)
	if err != nil {
		return err
	}

	fs, err := d.client.FormationList(appID)
	if err != nil {
		return err
	}

	if len(fs) == 1 && fs[0].ReleaseID != releaseID {
		stream, err := d.client.StreamJobEvents(appID, 0)
		if err != nil {
			return err
		}
		defer stream.Close()

		if err := d.client.PutFormation(&ct.Formation{
			AppID:     appID,
			ReleaseID: releaseID,
			Processes: fs[0].Processes,
		}); err != nil {
			return err
		}
		expect := make(jobEvents)
		for typ, n := range fs[0].Processes {
			expect[typ] = map[string]int{"up": n}
		}
		if _, _, err := waitForJobEvents(stream.Events, expect); err != nil {
			return err
		}
		if err := d.client.DeleteFormation(appID, fs[0].ReleaseID); err != nil {
			return err
		}
	}
	return nil
}
