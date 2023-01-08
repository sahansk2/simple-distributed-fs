package mp3util

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
)

func ConfigureLogger(hostname string, logLevelFlag string, dumptofile bool) {
	logLevel, err := logrus.ParseLevel(logLevelFlag)
	if err != nil {
		fmt.Println(err)
	}

	NodeLogger = logrus.New().WithFields(logrus.Fields{
		"hostname": hostname,
	}).Logger
	NodeLogger.SetReportCaller(true)
	NodeLogger.SetLevel(logLevel)
	if dumptofile {
		f, _ := os.OpenFile("mp3.log", os.O_WRONLY|os.O_CREATE, 0755)
		os.Stderr = f
		logrus.SetOutput(f)
		NodeLogger.SetOutput(f)
	}
}

func ConfigureUserLogger() {

}

var NodeLogger *logrus.Logger
var UserLogger *logrus.Logger
