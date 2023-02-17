const fs = require('fs')
const path = require('path')

const readline = require('readline')
const dirname = './data'
const outDirName = './csv'

// Create service client module using ES6 syntax.
const { S3Client, ListObjectsCommand, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');

// Set the AWS Region.
const REGION = "us-east-1";
// Create an Amazon S3 service client object.
const s3Client = new S3Client({ region: REGION });

const instruments = [
  // 'ABI',
  'EXIS',
  'MAG',
  'SEISS',
  // 'SUVI'
]

const dates = [
  '20221225',
  '20221226',
  '20221227',
  '20221228',
  '20221229',
  '20221230',
  '20221231'
]

const bucket = 'lzssc-s3-sbx'
const patKey = 'outgoing/op/GOES-17/l1b/INSTRUMENT/2022/dec/DATE/'

const saveContents = async (key, data) => {
  const fileName = `./${key}`
  const dirname = path.dirname(fileName)
  console.log(dirname, fileName)

  if (!fs.existsSync(dirname)) {
    fs.mkdirSync(dirname, { recursive: true })
  }
  fs.writeFileSync(fileName, data)
  console.log('Created', fileName)
}

const DownloadFile = async (key, count) => {
  console.log("Downloading", count, key)
  const command = new GetObjectCommand({
    Bucket: bucket,
    Key: key
  });
  const response = await s3Client.send(command);
  const stream = response.Body
  const data = Buffer.concat(await stream.toArray())
  console.log(count, key, data.length)
  await saveContents(key, data)
}

const downloadFolder = async (folder, count, StartAfter) => {
  let key, command
  console.log('DownloadFolder', StartAfter)
  if (StartAfter) {
    command = new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: folder,
      StartAfter
    });
  } else {
    command = new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: folder
    });
  }

  const response = await s3Client.send(command);
  for (let r of response.Contents) {
    key = r.Key
    await DownloadFile(key, count)
    count++
  }

  if (response.IsTruncated) {
    await downloadFolder(folder, count, key)
  } else {
    console.log('not truncated')
    delete response.Contents
    console.log(response)
  }
  // console.log(response)
}

const downloadS3Data = async () => {
  for (let instrument of instruments) {
    for (let d of dates) {
      let key = patKey.replace('INSTRUMENT', instrument)
      key = key.replace('DATE', d)
      count = 1
      startAfter = null
      await downloadFolder(key, count, startAfter)
    }
  }
}
downloadS3Data()