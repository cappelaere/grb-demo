'use strict'

const { Client } = require('@elastic/elasticsearch')
const { assert } = require('console')
// const { jwtVerifier } = require('./jwt.js')

module.exports = {
    R2PresignedUrl: async (bucket, key) => {
        const { R2PresignedUrl } = await import("./store.mjs")
        return await R2PresignedUrl(bucket, key)
    },
    S3PresignedUrl: async (bucket, key) => {
        const { S3PresignedUrl } = await import("./store.mjs")
        return await S3PresignedUrl(bucket, key)
    }
}

const ES_CLOUD_ID = process.env.ES_CLOUD_ID
const ES_USER = process.env.ES_USER
const ES_PWD = process.env.ES_PWD
const options = {
    cloud: { id: ES_CLOUD_ID },
    auth: {
        username: ES_USER,
        password: ES_PWD
    }
}
assert(ES_CLOUD_ID)
assert(ES_USER)
assert(ES_PWD)
// console.log(options)

const esClient = new Client(options)

const SEARCH_DCS_CIDS_INDEX = 'search-dcs-cids'
const SEARCH_DCS_GOES_INDEX = 'search-dcs-goes'
const SEARCH_DCS_IRIDIUM_INDEX = 'search-dcs-iridium'

const SEARCH_GRB_CIDS_INDEX = 'search-grb-cids'
const SEARCH_GRB_INDEX = 'search-grb-docs'

const IndexDocument = async (index, document, id) => {
    try {
        if (id) {
            const result = await esClient.index({
                index,
                id,
                document
            })
            //console.log(result)
        } else {
            const result = await esClient.index({
                index,
                document
            })
            // console.log(result)
        }
    } catch (err) {
        console.error(err)
    }
}

const SearchIndex = async (index, cid) => {
    try {
        const result = await esClient.search({
            index,
            query: {
                match: { cid }
            }
        })
        // const results = result.hits.hits.map((r) => r._source)
        const results = result.hits.hits.map((r) => {
            const id = r._id
            const result = r._source
            result.id = id
            return result
        })

        console.log(results)
        return results
    } catch (err) {
        console.error(err)
        return null
    }
}

const QueryIndex = async (index, query) => {
    try {
        const result = await esClient.search({
            index,
            query
        })
        const results = result.hits.hits.map((r) => r._source)
        return results
    } catch (err) {
        console.error(err)
        return null
    }
}

const QueryDcs = async (query, userFields, userLimit) => {
    const fields = userFields || ['cid']
    const formattedQuery = FormatQuery(query)
    const limit = parseInt(userLimit)

    // console.log(`QueryDcs: ${JSON.stringify(formattedQuery)}`)
    const results = await QueryIndex(SEARCH_DCS_INDEX, formattedQuery)
    console.log(results)
    const res = results.map((r) => {
        if (fields.length === 1) {
            if (fields[0] === 'all') {
                return r
            } else {
                return r[fields[0]]
            }
        }

        // remove extra fields
        const keys = Object.keys(r)
        for (var k of keys) {
            if (!fields.includes(k)) {
                // console.log(`deleting ${k} ${fields}`)
                delete r[k]
            }
        }
        return r
    })

    if (limit < res.length) {
        return res.slice(0, limit)
    } else {
        return res
    }
}

const DeleteByQuery = async (index, cid) => {
    try {
        const result = await esClient.deleteByQuery({
            index,
            refresh: true,
            query: {
                match: { cid }
            }
        })
        console.log(result)
        return result
    } catch (err) {
        console.error(err)
    }
}


//
// Index Cid Document with storage information
//
const IndexCid = async (document) => {
    const result = await IndexDocument(SEARCH_GRB_CIDS_INDEX, document, null)
    return result
}

const SearchCid = async (cid) => {
    return await SearchIndex(SEARCH_GRB_CIDS_INDEX, cid)
}

const DeleteCid = async (cid) => {
    return await DeleteByQuery(SEARCH_GRB_CIDS_INDEX, cid)
}

module.exports.IndexCid = IndexCid
module.exports.IndexDocument = IndexDocument
module.exports.SearchCid = SearchCid
module.exports.DeleteCid = DeleteCid
