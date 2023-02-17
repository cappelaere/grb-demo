const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3')
const { IndexCid } = require('./es.js')
const moment = require('moment')

const AWS_REGION = process.env.AWS_REGION || 'us-east-1'
const AWS_ACCOUNT = process.env.AWS_ACCOUNT

const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID
const R2_KEY = process.env.R2_KEY
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY

const s3Client = new S3Client({ region: AWS_REGION });

const r2Client = new S3Client({
    region: "auto",
    endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: R2_KEY,
        secretAccessKey: R2_SECRET_ACCESS_KEY
    },
})

const StoreData = async (cid, storageClass, bucket, key, contents, contentType) => {
    let client, account
    switch (storageClass) {
        case 's3':
            client = s3Client
            account = AWS_ACCOUNT
            break
        case 'r2':
            client = r2Client
            account = R2_ACCOUNT_ID
            break
        default:
            console.error(`Invalid Storage Class ${storageClass}`)
            return
    }

    console.log(`Storing ${storageClass} : ${bucket} / ${key}`)
    const input = {
        Bucket: bucket,
        Key: key,
        Body: contents,
        ContentType: contentType
    }
    try {
        const command = new PutObjectCommand(input);
        const response = await client.send(command);
        // console.log(response)

        const size = contents.length
        const mtime = moment().valueOf()
        const doc = {
            cid,
            class: storageClass,
            account,
            bucket,
            key,
            size,
            mtime,
            type: contentType
        }
        // console.log(doc)
        await IndexCid(doc)
        return response.$metadata.httpStatusCode
    } catch (err) {
        console.error(err)
        return 500  // internal error
    }
}

const StoreS3 = async (cid, bucket, key, contents, contentType) => {
    await StoreData(cid, 's3', bucket, key, contents, contentType)
    console.log(`Store in S3 bucket: ${bucket}, key:${key} ${contentType}`)
}

const StoreR2 = async (cid, bucket, key, contents, contentType) => {
    await StoreData(cid, 'r2', bucket, key, contents, contentType)
    console.log(`Store in R2 bucket: ${bucket}, key: ${key} ${contentType}`)
}

module.exports.StoreData = StoreData
module.exports.StoreS3 = StoreS3
module.exports.StoreR2 = StoreR2
