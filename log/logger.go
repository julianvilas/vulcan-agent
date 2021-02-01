package log

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type NullLog struct {
}

func (n *NullLog) Debugf(format string, args ...interface{}) {

}

func (n *NullLog) Infof(format string, args ...interface{}) {

}

func (n *NullLog) Errorf(format string, args ...interface{}) {

}
