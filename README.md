# DragonDrop
A simple drag and drop file uploader to S3, written for Deno in Typescript.

## S3 Setup

Create a bucket. You must enable public access and enable ACLs on the bucket 
and use a public policy:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::dragondrop1701/*"
        }
    ]
}
```

We must allow access to web browsers via cors. By default, only a small set of 
headers are exposed to the client, such as  `Cache-Control`, `Content-Language`, 
`Content-Type`, `Expires`, `Last-Modified`, and `Pragma`. Here we also add 
`ETag` and `Content-Length`:
```json
[
    {
        "AllowedHeaders": [
            "*"
        ],
        "AllowedMethods": [
            "GET",
            "HEAD"
        ],
        "AllowedOrigins": [
            "*"
        ],
        "ExposeHeaders": [
            "ETag",
            "Content-Length"
        ]
    }
]
```

After the bucket is created, we need to create a user group with policy over
the S3 bucket:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::[BUCKET_NAME]/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::[BUCKET_NAME]"
      ]
    }
  ]
}
```

Now create a user, and generate an access key id and secret access key.
We will add this to a file `~/.aws/credentials`:
```
[default]
aws_access_key_id = [KEY]
aws_secret_access_key = [SECRET]
```