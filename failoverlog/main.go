package failoverlog

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
	Path string
	FilterExp string
}

func (c *Ctx) Run() {
	f , err := os.Stat(c.Path)
	if err != nil {
		utils.HandleError(err)
	}

	if !f.IsDir() {
		fmt.Println("Must provide data directory and not file")
		os.Exit(1)
	}

	if c.FilterExp != "" {
		vbids, err := utils.ParseFilterExpresion(c.FilterExp)
		if err != nil {
			utils.HandleError(err)
		}

		c.printFiltered(vbids)
		return
	}

	c.walk()
}

func (c *Ctx) printFiltered(vbids []string) {
	for k, _ := range vbids {
		_, err := os.Stat(filepath.Join(c.Path, "failoverlog_"+vbids[k]+".fol"))
		if err != nil {
			fmt.Println("Failover log file for vbid (" + vbids[k]+ ") could not be found")
			continue
		}

		b, err := ioutil.ReadFile(filepath.Join(c.Path, "failoverlog_"+vbids[k]+".fol"))
		if err != nil {
			fmt.Printf("Error reading %s: %s\n", "failoverlog_"+vbids[k]+".fol", err.Error())
		}

		log, err := unpackFailoverLog(b)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		fmt.Printf("(vbid %s) %s\n", vbids[k], log.String())
	}
}

func (c *Ctx) walk() {
	files, err := ioutil.ReadDir(c.Path)
	if err != nil {
		utils.HandleError(err)
	}

	for _, f := range files {
		if match, _ := regexp.MatchString("failoverlog_([\\d]+).fol", f.Name()); match {
			vbid := f.Name()[12:len(f.Name()) - 4]
			b, err := ioutil.ReadFile(filepath.Join(c.Path, f.Name()))
			if err != nil {
				fmt.Printf("Error reading %s: %s\n", f.Name(), err.Error())
			}

			log, err := unpackFailoverLog(b)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}

			fmt.Printf("(vbid %s) %s\n", vbid, log.String())
		}
	}
}

type FailoverEntry struct {
	seqno uint64
	uuid  uint64
}

type FailoverLog struct {
	Log  []*FailoverEntry
	Size int
}

type FailoverLogError struct {
	reason string
}

func (fe *FailoverLogError) Error() string {
	return  fe.reason
}

func NewFailoverLog() *FailoverLog {
	rv := &FailoverLog{
		Log:  make([]*FailoverEntry, 0),
		Size: 0,
	}

	return rv
}

func (f *FailoverLog) AddEntry(entry *FailoverEntry) error {
	if f.Size != 0 && entry.seqno < f.Log[f.Size-1].seqno {
		return &FailoverLogError{"Seqno must be higher than seqno in last log entry"}
	}

	f.Log = append(f.Log, entry)
	f.Size++

	return nil
}

func (f *FailoverLog) String() string {
	s := ""
	for i := 0; i < f.Size; i++ {
		if i != 0 {
			s += ","
		}
		s += fmt.Sprintf(" {seq: %d, uuid: %d}", f.Log[i].seqno, f.Log[i].uuid)
	}

	return s
}

func unpackFailoverLog(data []byte) (*FailoverLog, error) {
	log := NewFailoverLog()

	buffer := bytes.NewBuffer(data)
	iterations := int(len(data) / 16)

	for i := 0; i < iterations; i++ {
		entry := &FailoverEntry{0, 0}
		err := binary.Read(buffer, binary.BigEndian, &entry.seqno)
		if err != nil {
			return nil, &FailoverLogError{"Could not read failover log entry "}
		}

		err = binary.Read(buffer, binary.BigEndian, &entry.uuid)
		if err != nil {
			return nil, &FailoverLogError{"Could not read failover log entry "}
		}
		if err := log.AddEntry(entry); err != nil {
			return nil, err
		}
	}

	return log, nil
}