const fs = require('fs')
const readline = require('readline')
const dirname = './data'
const outDirName = './csv'

// Create service client module using ES6 syntax.
const { S3Client, ListObjectsV2Command } = require('@aws-sdk/client-s3');

// Set the AWS Region.
const REGION = "us-east-1";
// Create an Amazon S3 service client object.
const s3Client = new S3Client({ region: REGION });

const instruments = [
    'ABI',
    // 'EXIS',
    // 'MAG',
    // 'SEISS',
    // 'SUVI'
]

const dates = [
    '20221225',
    // '20221226',
    // '20221227',
    // '20221228',
    // '20221229',
    // '20221230',
    // '20221231',
]

const bucket = 'lzssc-s3-sbx'
const patKey = 'outgoing/op/GOES-17/l1b/INSTRUMENT/2022/dec/DATE/'

const downloadS3Data = async () => {
    for (let instrument of instruments) {
        for (let d of dates) {
            let key = patKey.replace('INSTRUMENT', instrument)
            key = key.replace('DATE', d)
            console.log(key)
            const command = new ListObjectsV2Command({
                Bucket: bucket,
                Prefix: key
            });
            const response = await s3Client.send(command);
            console.log(response)
        }
    }
}
downloadS3Data()

const ProcessLine = (line) => {
    // console.log(line)
    const ts = line.slice(0, 22).trim()
    const size = line.slice(22, 30).trim()
    const f = line.slice(30, line.length).trim()
    return `${ts},${size},${f}\n`
}

const convertFile = async (dirname, filename) => {
    const fName = `${dirname}/${filename}`
    console.log(fName)

    const lineReader = readline.createInterface({
        input: fs.createReadStream(fName)
    })
    let lineno = 0
    let csv = ""
    for await (const line of lineReader) {
        csv += ProcessLine(line);
    }
    let outFileName = `${outDirName}/${filename.replace('.dat', '.csv')}`
    fs.writeFileSync(outFileName, csv)
    console.log(`Written ${outFileName}`)
}

const readAll = async () => {
    fs.readdir(dirname, async (err, files) => {
        if (err)
            console.log(err);
        else {
            console.log("\nCurrent directory filenames:");
            for (const file of files) {
                await convertFile(dirname, file);
            }
        }
    })
}

