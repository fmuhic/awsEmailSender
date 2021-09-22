package main

import (
    "time"
    "fmt"
    "os"
    // "reflect"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/ses"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"
    "github.com/aws/aws-sdk-go/service/sqs"

    "github.com/fmuhic/emailSender/src/message"
    "github.com/fmuhic/emailSender/src/consumer"
    "github.com/fmuhic/emailSender/src/storage"
    "github.com/fmuhic/emailSender/src/email"
    "github.com/fmuhic/emailSender/src/config"
)

func main() {
    appConfig, configErr := config.LoadFromFile("./config.json")
    if configErr != nil {
        fmt.Printf("Error while loading config file: %v", configErr)
        os.Exit(1)
    }

    msgQueue := message.NewMessageQueue()

    sess, _ := session.NewSession(&aws.Config{
        Region: aws.String(appConfig.Aws.Region),
        Credentials: credentials.NewSharedCredentials("", appConfig.Aws.Profile),
    })

    simpleEmailService := ses.New(sess)
    emailService := email.NewEmailSender(
        simpleEmailService,
        msgQueue,
        appConfig.EmailSender,
    )
    go emailService.Run()


    go func(mq *message.MessageQueue) {
        for {
            fmt.Printf("Queue size is %v\n", mq.Len())
			time.Sleep(3 * time.Second)
        }
    }(msgQueue)

    s3Downloader := s3manager.NewDownloader(sess)
    cloudStorage := storage.NewCloudStorage(s3Downloader)

    sqsClient := sqs.New(sess)

    c := consumer.NewConsumer(
        sqsClient,
        msgQueue,
        cloudStorage,
        appConfig.Consumer,
    )
    c.Run()
}
