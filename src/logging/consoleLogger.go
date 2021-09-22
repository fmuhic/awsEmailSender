package logging

import (
    "fmt"
    "time"

    "github.com/fatih/color"
)

type ConsoleLogger struct {
    showDebug bool
}

func NewConsoleLogger(showDebug bool) *ConsoleLogger {
    return &ConsoleLogger{
        showDebug: showDebug,
    }
}

func (cl *ConsoleLogger) Debug(msg string, args ...interface{}) {
    green := color.New(color.FgGreen).SprintFunc()
    if cl.showDebug {
        fmt.Printf("%v: %v\n", cl.getTime(), green(fmt.Sprintf(msg, args...)))
    }
}

func (cl *ConsoleLogger) Info(msg string, args ...interface{}) {
    yellow := color.New(color.FgYellow).SprintFunc()
    fmt.Printf("%v: %v\n", cl.getTime(), yellow(fmt.Sprintf(msg, args...)))
}

func (cl *ConsoleLogger) Error(msg string, args ...interface{}) {
    red := color.New(color.FgRed).SprintFunc()
    fmt.Printf("%v: %v\n", cl.getTime(), red(fmt.Sprintf(msg, args...)))
}

func (cl *ConsoleLogger) getTime() string {
    blue := color.New(color.FgBlue).SprintFunc()
    dt := time.Now()
    return blue(dt.Format("2006-01-02 15:04:05.000"))
}
