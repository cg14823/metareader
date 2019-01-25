package snapshot

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"metareader/utils"
	"os"
	"path/filepath"
	"regexp"
)

type Ctx struct {
	Path      string
	FilterExp string
}

type SnapError struct {
	reason string
}

func (se *SnapError) Error() string {
	return se.reason
}


func (s *Ctx) Run() {
	f , err := os.Stat(s.Path)
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

	for _, f := range files {
		if match, _ := regexp.MatchString("snapshot_([\\d]+).snp", f.Name()); match {
			vbid := f.Name()[9:len(f.Name()) - 4]
			b, err := ioutil.ReadFile(filepath.Join(s.Path, f.Name()))
			if err != nil {
				fmt.Printf("Error reading %s: %s\n", f.Name(), err.Error())
			}

			out, err := snapshotHumanReadableFormat(vbid, b)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}

			fmt.Println(out)
		}
	}
}

func (s *Ctx) printFiltered(vbids []string) {
	for k, _ := range vbids {
		_, err := os.Stat(filepath.Join(s.Path, "snapshot_"+vbids[k]+".snp"))
		if err != nil {
			fmt.Println("Snapshot file for vbid (" + vbids[k]+ ") could not be found")
			continue
		}

		b, err := ioutil.ReadFile(filepath.Join(s.Path, "snapshot_"+vbids[k]+".snp"))
		if err != nil {
			fmt.Printf("Error reading %s: %s\n", "snapshot_"+vbids[k]+".snp", err.Error())
		}

		out, err := snapshotHumanReadableFormat(vbids[k], b)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		fmt.Println(out)
	}
}

func snapshotHumanReadableFormat(vbid string, snapBytes []byte) (string, error) {
	buffer := bytes.NewBuffer(snapBytes)
	var start uint64
	var end uint64
	var seqNo uint64
	err := binary.Read(buffer, binary.BigEndian, &start)
	if err != nil {
		return "", &SnapError{"could not read file due to: " + err.Error()}
	}
	err = binary.Read(buffer, binary.BigEndian, &end)
	if err != nil {
		return "", &SnapError{"could not read file due to: " + err.Error()}
	}
	err = binary.Read(buffer, binary.BigEndian, &seqNo)
	if err != nil {
		return "", &SnapError{"could not read file due to: " + err.Error()}
	}
	return fmt.Sprintf("(vBucket %s) start: %d end: %d lastSeqNo: %d", vbid, start, end, seqNo), nil
}

