package main

import (
	"flag"
	"fmt"
	"github.com/Wessie/appdirs"
	"github.com/boramalper/magnetico/pkg/persistence"
	"github.com/jessevdk/go-flags"
	"go.uber.org/zap"
	"log"
	"os"
)

var verbose int
var cmdFlags struct {
	Driver1      string `long:"driver1"      description:"driver of the (magneticod) database 1"`
	DataSource1  string `long:"database1"    description:"data source of the (magneticod) database 1"`
	Driver2      string `long:"driver2"      description:"driver of the (magneticod) database 2"`
	DataSource2  string `long:"database2"    description:"data source of the (magneticod) database 2"`

	Progress     float32 `short:"p" long:"progress" description:"Continue from progress(percentage)."`
	Verbose      []bool `short:"v" long:"verbose" description:"Increases verbosity."`
}


func migrateParseFlags()  {
	flag.Usage = func() {
		_, _ = fmt.Fprintf(os.Stderr, "Migrate data from db1 to db2\n")
		flag.PrintDefaults()
	}
	if _, err := flags.Parse(&cmdFlags); err != nil {
		if  err.(*flags.Error).Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			panic(err)
		}
	}

	if cmdFlags.Driver1 == "" {
		cmdFlags.Driver1 = "sqlite3"
	}
	if cmdFlags.DataSource1 == "" {
		cmdFlags.DataSource1 =
			appdirs.UserDataDir("magneticod", "", "", false) +
				"/database.sqlite3" +
				"?_journal_mode=WAL" // https://github.com/mattn/go-sqlite3#connection-string
	}

	if cmdFlags.Driver2 == "" {
		cmdFlags.Driver2 = "es"
	}
	if cmdFlags.DataSource2 == "" {
		cmdFlags.DataSource2 = "http://127.0.0.1:9200/"
	}
	verbose = len(cmdFlags.Verbose)
}

func migrate() {
	migrateParseFlags()
	db1, err := persistence.MakeDatabase(cmdFlags.Driver1, cmdFlags.DataSource1, zap.L())
	if err != nil {
		panic(err)
	}
	db2, err := persistence.MakeDatabase(cmdFlags.Driver2, cmdFlags.DataSource2, zap.L())
	if err != nil {
		panic(err)
	}

	count, err := db1.GetNumberOfTorrents()
	if err != nil {
		panic(err)
	}
	var i = uint(float32(count) * cmdFlags.Progress / 100.0)
	for i < count {
		infoHash, name, files, discorveredTime, err := db1.QueryTorrentById(i)
		if err == nil && infoHash != nil {
			err = db2.AddNewTorrent(infoHash, name, files, discorveredTime)
			if err != nil {
				_ = db1.Close()
				_ = db2.Close()
				panic(err)
			}
		}
		i ++
		if i % (count/1000) == 0 {
			log.Printf("Progress: %.01f/100", float32(100 * i) / float32(count))
		}
	}

}