package email

import (
    "fmt"
    "time"
    "sync"
    // "math/rand"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/service/ses"
    "github.com/aws/aws-sdk-go/aws/awserr"

    "github.com/fmuhic/emailSender/src/message"
    "github.com/fmuhic/emailSender/src/config"
)

type EmailSender struct {
    sesClient *ses.SES
    messages *message.MessageQueue
    config config.EmailSender
}

func NewEmailSender(s *ses.SES, mq *message.MessageQueue, config config.EmailSender) *EmailSender {
    return &EmailSender{
        sesClient: s,
        messages: mq,
        config: config,
    }
}

func (es *EmailSender) Run() {
    for {
        msgs := es.messages.PopMany(es.config.MaxMsgsPerSec)
		if len(msgs) > 0 {
            start := time.Now()

            var wg sync.WaitGroup
            for _, msg := range msgs {
                wg.Add(1)
                go es.sendEmail(msg, &wg)
            }
            wg.Wait()

            elapsed := time.Since(start).Milliseconds()
            fmt.Printf("Message batch took %v ms\n", elapsed)

            if 1000 - elapsed > 0 {
                fmt.Printf("Email Sender: sleeping for %v ms\n", 1000 - elapsed)
                time.Sleep(time.Duration(1000 - elapsed) * time.Millisecond)
            }
        } else {
            fmt.Println("Email Sender: No emails to send, idling ...")
			time.Sleep(time.Duration(es.config.IdleDurationInSec) * time.Second)
		}
    }
}

func (es *EmailSender) sendEmail(msg *message.SQSMessage, wg *sync.WaitGroup) {
    defer wg.Done()

    input := &ses.SendEmailInput{
        Destination: &ses.Destination{
            CcAddresses: []*string{},
            ToAddresses: []*string{aws.String(msg.To)},
        },
        Message: &ses.Message{
            Body: &ses.Body{
                Html: &ses.Content{
                    Charset: aws.String("UTF-8"),
                    Data:    msg.Template,
                },
            },
            Subject: &ses.Content{
                Charset: aws.String("UTF-8"),
                Data:    aws.String(msg.Subject),
            },
        },
        Source: aws.String(msg.From),
    }

    result, err := es.sesClient.SendEmail(input)

    if err != nil {
        if aerr, ok := err.(awserr.Error); ok {
            switch aerr.Code() {
                case ses.ErrCodeMessageRejected:
                    fmt.Println(ses.ErrCodeMessageRejected, aerr.Error())
                case ses.ErrCodeMailFromDomainNotVerifiedException:
                    fmt.Println(ses.ErrCodeMailFromDomainNotVerifiedException, aerr.Error())
                case ses.ErrCodeConfigurationSetDoesNotExistException:
                    fmt.Println(ses.ErrCodeConfigurationSetDoesNotExistException, aerr.Error())
                default:
                    fmt.Println(aerr.Error())
            }
        } else {
            fmt.Println(err.Error())
        }

        return
    }

    fmt.Println("Email Sent: " + msg.TemplateName)
    fmt.Println(result)
}

// Simulate Email sending process
// func (es *EmailSender) sendEmail(msg *message.SQSMessage, wg *sync.WaitGroup) {
    // defer wg.Done()

    // rand.Seed(time.Now().UnixNano())
    // n := rand.Intn(400)
    // time.Sleep(time.Duration(n)*time.Millisecond)
    // fmt.Printf("Email sent. Process took %v ms\n", n)
// }

