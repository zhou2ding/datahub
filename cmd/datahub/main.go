package main

import (
	zap "datahub/internal/log"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport/grpc"

	"datahub/internal/conf"

	_ "go.uber.org/automaxprocs"
)

// go build -ldflags "-X main.Version=x.y.z"
var (
	// Name is the name of the compiled software.
	Name string
	// Version is the version of the compiled software.
	Version string
	// BuildDate is the build time of the compiled software.
	BuildDate string
	// flagconf is the config flag.
	flagconf    string
	versionFlag bool

	id, _ = os.Hostname()
)

func init() {
	flag.StringVar(&flagconf, "c", "../../configs/config.yaml", "config path, eg: -c config.yaml")
	flag.BoolVar(&versionFlag, "v", false, "show version")
}

func newApp(logger log.Logger, gs *grpc.Server) *kratos.App {
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{"build": BuildDate}),
		kratos.Logger(logger),
		kratos.Server(
			gs,
		),
	)
}

func main() {
	flag.Parse()
	if versionFlag {
		fmt.Printf("App: %s Version: %s, BuildDate: %s\n", Name, Version, BuildDate)
		os.Exit(0)
	}

	c := config.New(
		config.WithSource(
			file.NewSource(flagconf),
		),
	)
	defer c.Close()

	if err := c.Load(); err != nil {
		panic(err)
	}

	var bc conf.Bootstrap
	if err := c.Scan(&bc); err != nil {
		panic(err)
	}
	logger := zap.NewLogger(&zap.Config{
		Level:      bc.Log.Level,
		Filename:   strings.TrimRight(bc.Log.Path, "/") + "/" + "datahub.log",
		MaxSize:    int(bc.Log.Size),
		MaxBackups: int(bc.Log.Limit),
		MaxAge:     int(bc.Log.Expire),
		Compress:   true,
		Stdout:     bc.Log.Stdout,
	})
	log.SetLogger(logger)

	app, cleanup, err := wireApp(bc.Server, bc.Data, bc.Log, logger)
	if err != nil {
		panic(err)
	}
	defer cleanup()

	go func() {
		if err = app.Run(); err != nil {
			panic(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	if err = app.Stop(); err != nil {
		panic(err)
	} else {
		log.Infof("%s stopped", Name)
	}
}
