package snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"

	"github.com/cg14823/metareader/utils"
)

type Ctx struct {
	Path       string
	FilterExp  string
	JsonFormat bool
}

type Snap struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
	SeqNo uint64 `json:"seqNo"`
}

type SnapOut struct {
	Snapshots map[string]*Snap `json:"snapshots"`
}

type SnapError struct {
	reason string
}

func (se *SnapError) Error() string {
	return se.reason
}

func (s *Ctx) Run() {
	f, err := os.Stat(s.Path)
	if err != nil {
		utils.HandleError(err)
	}

	if !f.IsDir() {
		utils.HandleError(&SnapError{"Must provide data directory and not file"})
	}

	if s.FilterExp != "" {
		vbids, err := utils.ParseFilterExpresion(s.FilterExp)
		if err != nil {
			utils.HandleError(err)
		}

		s.printFiltered(vbids)
		return
	}

	s.walk()
}

func (s *Ctx) walk() {
	files, err := ioutil.ReadDir(s.Path)
	if err != nil {
		utils.HandleError(err)
	}

	finalOutput := ""
	jsonOut := &SnapOut{}
	if s.JsonFormat {
		jsonOut.Snapshots = make(map[string]*Snap)
	}

	for _, f := range files {
		if match, _ := regexp.MatchString("snapshot_([\\d]+).snp", f.Name()); match {
			vbid := f.Name()[9 : len(f.Name())-4]
			b, err := ioutil.ReadFile(filepath.Join(s.Path, f.Name()))
			if err != nil {
				fmt.Printf("Error reading %s: %s\n", f.Name(), err.Error())
				continue
			}

			if s.JsonFormat {
				if snap, err := unpackSnapshot(b); err == nil {
					jsonOut.Snapshots[vbid] = snap
				}
			} else {
				out, err := snapshotHumanReadableFormat(vbid, b)
				if err != nil {
					fmt.Println(err.Error())
					continue
				}

				finalOutput += out + "\n"
			}
		}
	}

	if s.JsonFormat {
		data, err := json.Marshal(jsonOut)
		if err != nil {
			fmt.Println("ERROR: Could not retrieve snapshot markers")
			os.Exit(1)
		}

		fmt.Printf("%s\n", data)
	} else {
		fmt.Println(finalOutput)
	}
}

func (s *Ctx) printFiltered(vbids []string) {
	finalOutput := ""
	jsonOut := &SnapOut{}
	if s.JsonFormat {
		jsonOut.Snapshots = make(map[string]*Snap)
	}

	for k, _ := range vbids {
		_, err := os.Stat(filepath.Join(s.Path, "snapshot_"+vbids[k]+".snp"))
		if err != nil {
			if !s.JsonFormat {
				fmt.Println("Snapshot file for vbid (" + vbids[k] + ") could not be found")
			}
			continue
		}

		b, err := ioutil.ReadFile(filepath.Join(s.Path, "snapshot_"+vbids[k]+".snp"))
		if err != nil {
			fmt.Printf("Error reading %s: %s\n", "snapshot_"+vbids[k]+".snp", err.Error())
		}

		if s.JsonFormat {
			if snap, err := unpackSnapshot(b); err == nil {
				jsonOut.Snapshots[vbids[k]] = snap
			}
		} else {
			out, err := snapshotHumanReadableFormat(vbids[k], b)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}

			finalOutput += out + "\n"
		}
	}

	if s.JsonFormat {
		data, err := json.Marshal(jsonOut)
		if err != nil {
			fmt.Println("ERROR: Could not retrieve snapshot markers")
			os.Exit(1)
		}

		fmt.Printf("%s\n", data)
	} else {
		fmt.Println(finalOutput)
	}
}

func unpackSnapshot(snapBytes []byte) (*Snap, error) {
	buffer := bytes.NewBuffer(snapBytes)
	snap := &Snap{}
	err := binary.Read(buffer, binary.BigEndian, &snap.Start)
	if err != nil {
		return nil, &SnapError{"could not read file due to: " + err.Error()}
	}

	err = binary.Read(buffer, binary.BigEndian, &snap.End)
	if err != nil {
		return nil, &SnapError{"could not read file due to: " + err.Error()}
	}

	err = binary.Read(buffer, binary.BigEndian, &snap.SeqNo)
	if err != nil {
		return nil, &SnapError{"could not read file due to: " + err.Error()}
	}

	return snap, nil
}

func snapshotHumanReadableFormat(vbid string, snapBytes []byte) (string, error) {
	snap, err := unpackSnapshot(snapBytes)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("(vBucket %s) start: %d end: %d lastSeqNo: %d", vbid, snap.Start, snap.End, snap.SeqNo),
		nil
}
