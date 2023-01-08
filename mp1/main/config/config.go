package config

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
)

type Host struct {
	Addr string
	Port string
}

var VMIps = []Host{
	Host{"fa22-cs425-5101.cs.illinois.edu", "6969"},
	Host{"fa22-cs425-5102.cs.illinois.edu", "6969"},
	Host{"fa22-cs425-5103.cs.illinois.edu", "6969"},
	Host{"fa22-cs425-5104.cs.illinois.edu", "6969"},
	Host{"fa22-cs425-5105.cs.illinois.edu", "6969"},
	Host{"fa22-cs425-5106.cs.illinois.edu", "6969"},
	Host{"fa22-cs425-5107.cs.illinois.edu", "6969"},
	Host{"fa22-cs425-5108.cs.illinois.edu", "6969"},
	Host{"fa22-cs425-5109.cs.illinois.edu", "6969"},
	Host{"fa22-cs425-5110.cs.illinois.edu", "6969"},
}

const (
	// ArgMax : Run `getconf arg_max` teehe (real)
	ArgMax       = 10000
	DeleteMaFile = true
)

var QLogger *logrus.Logger = logrus.StandardLogger()

func ConfigureQLogger(logLevelFlag string) {
	logLevel, err := logrus.ParseLevel(logLevelFlag)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	QLogger = logrus.New()
	QLogger.SetLevel(logLevel)
}