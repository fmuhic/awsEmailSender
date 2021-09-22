package logging

type Logger interface {
    Debug(string, ...interface{})
    Info(string, ...interface{})
    Error(string, ...interface{})
}
