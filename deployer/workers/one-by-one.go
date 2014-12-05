package main

import (
	"github.com/flynn/flynn/controller/client"
	ct "github.com/flynn/flynn/controller/types"
)

type OneByOneDeployer struct {
	client *controller.Client
}

func (d *OneByOneDeployer) Deploy(appID, releaseID, prevReleaseID string) error {
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

		oldFormation := fs[0].Processes
		newFormation := map[string]int{}

		for typ, num := range fs[0].Processes {
			for i := 0; i < num; i++ {
				// stop one process
				oldFormation[typ]--
				if err := d.client.PutFormation(&ct.Formation{
					AppID:     appID,
					ReleaseID: fs[0].ReleaseID,
					Processes: oldFormation,
				}); err != nil {
					return err
				}
				if _, _, err := waitForJobEvents(stream.Events, jobEvents{typ: {"down": 1}}); err != nil {
					return err
				}
				// start one process
				newFormation[typ]++
				if err := d.client.PutFormation(&ct.Formation{
					AppID:     appID,
					ReleaseID: releaseID,
					Processes: newFormation,
				}); err != nil {
					return err
				}
				if _, _, err := waitForJobEvents(stream.Events, jobEvents{typ: {"up": 1}}); err != nil {
					return err
				}
			}
		}
		if err := d.client.DeleteFormation(appID, fs[0].ReleaseID); err != nil {
			return err
		}
	}
	return nil
}
