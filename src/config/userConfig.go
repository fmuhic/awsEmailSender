package config

import (
    "os"
    "encoding/json"
    "io/ioutil"
)

type AppConfig struct {
    Aws Aws
    Consumer Consumer
    EmailSender EmailSender
    Logger Logger
}

type Aws struct {
    Profile string
    Region string
}

type Consumer struct {
    QueueUrl string
    QueueMaxMsgsPerRequest int64
    MsgVisibilitySec int64
    LongPollingDurationSec int64
    IdleDurationInSec int
    MaxMsgQueueSize int
    WorkerCount int
}

type EmailSender struct {
    IdleDurationInSec int
    MaxMsgsPerSec int
}

type Logger struct {
    ShowDebug bool
}


func LoadFromFile(fileName string) (*AppConfig, error) {
    jsonFile, openErr := os.Open(fileName)
    if openErr != nil {
        return nil, openErr
    }
    defer jsonFile.Close()

    byteValue, _ := ioutil.ReadAll(jsonFile)
    var appConfig AppConfig
    decodeErr := json.Unmarshal(byteValue, &appConfig)
    if decodeErr != nil {
        return nil, decodeErr
    }

    return &appConfig, nil
}
