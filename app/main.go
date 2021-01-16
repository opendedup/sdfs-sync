package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"

	"crypto/sha256"
	"encoding/base64"

	sdfs "github.com/opendedup/sdfs-sync/sync"
	"github.com/sevlyar/go-daemon"
)

func main() {
	log.SetFlags(log.Lmicroseconds)
	// Scans the arg list and sets up flags
	debug := flag.Bool("debug", false, "print debugging messages.")
	daemonize := flag.Bool("d", false, "daemonize mount")
	configFile := flag.String("config", "config.yaml", "The configuration yaml for this setup.")
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Printf("usage: %s options\n", path.Base(os.Args[0]))
		fmt.Printf("\noptions:\n")
		flag.PrintDefaults()
		os.Exit(2)
	}
	if !isFlagPassed("config") {
		fmt.Printf("--config must be set")
	}
	cfg, err := sdfs.NewConfig(*configFile)
	if err != nil {
		log.Fatalf("Config Error(%s): %v\n", *configFile, err)
		os.Exit(3)
	}
	hasher := sha256.New()
	hasher.Write([]byte(cfg.Server.ServerURL))
	sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))
	listener, err := sdfs.NewsdfsListener(*configFile, *debug)
	if err != nil {
		log.Fatalf("Connection Error(%s): %v\n", *configFile, err)
		os.Exit(4)
	}
	if *daemonize {
		mcntxt := &daemon.Context{
			PidFileName: "/var/run/sdfsmount-" + sha + ".pid",
			PidFilePerm: 0644,
			LogFileName: "/var/log/sdfsmount-" + sha + ".log",
			LogFilePerm: 0640,
			WorkDir:     "/var/run/",
			Umask:       027,
		}
		d, err := mcntxt.Reborn()
		if err != nil {
			log.Fatal("Unable to run: ", err)
		}
		if d != nil {
			return
		}
		defer mcntxt.Release()

		log.Print("- - - - - - - - - - - - - - -")
		log.Print("sync daemon started")
		start(listener)
	} else {
		start(listener)
	}

}

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func start(listener *sdfs.Listener) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener.Listen(ctx)
}
