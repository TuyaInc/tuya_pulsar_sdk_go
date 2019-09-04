package pulsar

import "github.com/sirupsen/logrus"

func SetInternalLogLevel(level logrus.Level) {
	logrus.SetLevel(level)
}
