package message

type SQSMessage struct {
    Timestamp int64
    TemplateName string
    To string
    From string
    Subject string
    S3BucketName string
    S3BucketRegion string
    S3BucketEnv string
    Template *string
}
