// Upload file in outgoing directory to pat's bucket and R2
const fs = require('fs')
const path = require('path')
const { resolve } = require('path');
const { StoreS3, StoreR2 } = require('./storage.js')
const { IndexDocument } = require('./es.js')

const moment = require('moment')

const MAX_FILES = 0

const S3_BUCKET = 'geocloud-pgc'
const R2_BUCKET = 'geocloud-dcs'

const CID = require('cids')
const multihashing = require('multihashing-async')

const SEARCH_GRB_INDEX = 'search-grb-docs'

const dirname = './outgoing/'

const getFileList = async (dirName, fileList = []) => {
    const files = fs.readdirSync(dirName);
    console.log(dirName, files)

    for await (const file of files) {
        const filepath = path.join(dirName, file);
        const stat = fs.statSync(filepath);

        if (stat.isDirectory()) {
            fileList = await getFileList(filepath, fileList)
        } else {
            fileList.push(filepath);
        }
    }

    return fileList;
};

const ParseFileName = (f) => {
    const basename = path.basename(f, '.nc')
    const arr = basename.split("_")
    const product = arr[1] // EXIS-L1b-SFXR
    // console.log(product)
    const arr2 = product.split('-')
    // console.log(arr2)

    const instrument = arr2[0]
    const level = arr2[1]
    const satellite = arr[2]
    const creationdate = arr[5].replace('c', '')
    const ts = moment(creationdate, 'YYYYDDDHHmmss') // 2022 360 00 00 242
    const timestamp = ts.format()
    // console.log(timestamp)
    return {
        instrument, level, satellite, timestamp, product
    }
}

const GenerateCid = async (bytes) => {
    const hash = await multihashing(bytes, 'sha2-256')
    const cid = new CID(1, 'raw', hash)
    return cid.toString()
}

const UploadFile = async (f) => {
    console.log(`upload ${f}`)
    const { instrument, level, satellite, timestamp, product } = ParseFileName(f)

    const data = fs.readFileSync(f)
    const cid = await GenerateCid(data)
    const json = {
        timestamp,
        satellite,
        level,
        instrument,
        product,
        cid
    }
    // console.log(json)

    const contentType = 'application/x-netcdf'
    await StoreS3(cid, S3_BUCKET, f, data, contentType)
    await StoreR2(cid, R2_BUCKET, f, data, contentType)

    // await IndexDocument(SEARCH_GRB_INDEX, json, json.cid)
    // console.log(f)
    // console.log(JSON.stringify(json))
    // console.log(data.length)
}

const UploadAll = async () => {
    const files = await getFileList('./outgoing/op/GOES-17/l1b');
    for (let f of files) {
        await UploadFile(f)
    }
}

// UploadAll()
UploadFile('outgoing/op/GOES-17/l1b/EXIS/2022/dec/20221225/OR_EXIS-L1b-SFEU_G17_s20223590000300_e20223590001000_c20223590001053.nc')
