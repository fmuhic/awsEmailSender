package main

import (
    "fmt"
    "os"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/ses"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"
    "github.com/aws/aws-sdk-go/service/sqs"

    "github.com/fmuhic/emailSender/src/message"
    "github.com/fmuhic/emailSender/src/consumer"
    "github.com/fmuhic/emailSender/src/logging"
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

    var logger = logging.NewConsoleLogger(appConfig.Logger.ShowDebug)

    msgQueue := message.NewMessageQueue(128)

    sess, _ := session.NewSession(&aws.Config{
        Region: aws.String(appConfig.Aws.Region),
        Credentials: credentials.NewSharedCredentials("", appConfig.Aws.Profile),
    })

    simpleEmailService := ses.New(sess)
    emailService := email.NewEmailSender(
        simpleEmailService,
        msgQueue,
        appConfig.EmailSender,
        logger,
    )
    go emailService.Run()

    s3Downloader := s3manager.NewDownloader(sess)
    cloudStorage := storage.NewCloudStorage(s3Downloader)

    sqsClient := sqs.New(sess)

    c := consumer.NewConsumer(
        sqsClient,
        msgQueue,
        cloudStorage,
        appConfig.Consumer,
        logger,
    )
    c.Run()
}
