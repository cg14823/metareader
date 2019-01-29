/*
 * This repo is to print in a human readable format failover files and shapshot files produced by cbbackupmgr
**/
package main

import (
	"fmt"
	"github.com/couchbase/cbflag"
	"metareader/failoverlog"
	"metareader/snapshot"
	"os"
)

type MetaReaderCtx struct {
	version string
}

func (mctx *MetaReaderCtx) Run() {
	fmt.Println("metareader version", mctx.version)
}

func main() {
	metaCtx := &MetaReaderCtx{"0.0.0"}
	snapCtx := snapshot.Ctx{}
	failCtx := &failoverlog.Ctx{}

	cmdline := cbflag.CLI{
		Name: "metareader",
		Desc: "Read and present in human readable format snapshot and failover files",
		Run:  metaCtx.Run,
		Commands: []*cbflag.Command{
			&cbflag.Command{
				Name:     "snapshot",
				Desc:     "Read and display the snapshot file data",
				Run:      snapCtx.Run,
				Commands: []*cbflag.Command{},
				Flags: []*cbflag.Flag{
					cbflag.StringFlag(
						/* Destination  */ &snapCtx.Path,
						/* Default      */ "",
						/* Short Option */ "p",
						/* Long Option  */ "path",
						/* Env Variable */ "",
						/* Usage        */ "The directory used to store backup data",
						/* Deprecated   */ []string{},
						/* Validator    */ nil,
						/* Required     */ true,
						/* Hidden       */ false,
					),
					cbflag.StringFlag(
						/* Destination  */ &snapCtx.FilterExp,
						/* Default      */ "",
						/* Short Option */ "",
						/* Long Option  */ "filter",
						/* Env Variable */ "",
						/* Usage        */ "Comma separated list of vbuckets to read snapshot data"+
							", it also accepts ranges (e.g. 1-100,300)",
						/* Deprecated   */ []string{},
						/* Validator    */ nil,
						/* Required     */ false,
						/* Hidden       */ false,
					),
					cbflag.BoolFlag(
						/* Destination  */ &snapCtx.JsonFormat,
						/* Default      */ false,
						/* Short Option */ "j",
						/* Long Option  */ "json",
						/* Env Variable */ "",
						/* Usage        */ "Returns snapshot data as json",
						/* Deprecated   */ []string{},
						/* Hidden       */ false,
					),
				},
			},
			&cbflag.Command{
				Name:     "failoverlog",
				Desc:     "Read and display the snapshot file data",
				Run:      failCtx.Run,
				Commands: []*cbflag.Command{},
				Flags: []*cbflag.Flag{
					cbflag.StringFlag(
						/* Destination  */ &failCtx.Path,
						/* Default      */ "",
						/* Short Option */ "p",
						/* Long Option  */ "path",
						/* Env Variable */ "",
						/* Usage        */ "The directory used to store backup data",
						/* Deprecated   */ []string{},
						/* Validator    */ nil,
						/* Required     */ true,
						/* Hidden       */ false,
					),
					cbflag.StringFlag(
						/* Destination  */ &failCtx.FilterExp,
						/* Default      */ "",
						/* Short Option */ "",
						/* Long Option  */ "filter",
						/* Env Variable */ "",
						/* Usage        */ "Comma separated list of vbuckets to read failover log data"+
							", it also accepts ranges (e.g. 1-100,300)",
						/* Deprecated   */ []string{},
						/* Validator    */ nil,
						/* Required     */ false,
						/* Hidden       */ false,
					),
				},
			},
		},
		Writer: os.Stdout,
	}

	cmdline.Parse(os.Args)
}
