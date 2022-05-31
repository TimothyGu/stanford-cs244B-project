package zookeeper

import log "github.com/sirupsen/logrus"

type prefixHook string

func (p prefixHook) Levels() []log.Level {
	return log.AllLevels
}

func (p prefixHook) Fire(entry *log.Entry) error {
	entry.Message = string(p) + entry.Message
	return nil
}
