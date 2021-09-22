package email

import (
    "time"
    "sync"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/service/ses"
    "github.com/aws/aws-sdk-go/aws/awserr"

    "github.com/fmuhic/emailSender/src/message"
    "github.com/fmuhic/emailSender/src/logging"
    "github.com/fmuhic/emailSender/src/config"
)

type EmailSender struct {
    sesClient *ses.SES
    messages *message.MessageQueue
    config config.EmailSender
	logger logging.Logger
}

func NewEmailSender(s *ses.SES,
	mq *message.MessageQueue,
	config config.EmailSender,
	logger logging.Logger) *EmailSender {
	return &EmailSender{
		sesClient: s,
		messages:  mq,
		config:    config,
		logger:    logger,
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
			es.logger.Debug("Message batch took %v ms to send", elapsed)

            if 1000 - elapsed > 0 {
                time.Sleep(time.Duration(1000 - elapsed) * time.Millisecond)
            }
        } else {
			es.logger.Debug("No emails to send")
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

    _, err := es.sesClient.SendEmail(input)

	if err != nil {
		errorMsg := "Unable to send email: " + err.Error()
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
                case ses.ErrCodeMessageRejected:
                    errorMsg = ses.ErrCodeMessageRejected + ": " + aerr.Error()
                case ses.ErrCodeMailFromDomainNotVerifiedException:
                    errorMsg = ses.ErrCodeMailFromDomainNotVerifiedException + ": " + aerr.Error()
                case ses.ErrCodeConfigurationSetDoesNotExistException:
                    errorMsg = ses.ErrCodeConfigurationSetDoesNotExistException + ": " + aerr.Error()
                default:
                    errorMsg = "SES error: " + aerr.Error()
			}
		}

		es.logger.Error(errorMsg)
		return
	}

	es.logger.Info("Email sent: " + msg.TemplateName)
}
