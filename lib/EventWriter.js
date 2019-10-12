const r = require.main.rethinkdb || require('rethinkdb')

class EventWriter {
  constructor(service, name) {
    this.name = name
    this.service = service
    this.started = false

    this.tableName = this.name + "_events"

    this.lastInsertedId = 0

    this.outputQueue = []
  }

  writeEvents(events, origin) {
    if(!(events instanceof Array)) events = [events]
    let eventsBucket
    if(events.length == 1) {
      eventsBucket = events[0]
      eventsBucket.timestamp = new Date()
      eventsBucket.origin = origin
    } else {
      eventsBucket = this.createEventsBucket(events, origin)
    }
    if(!this.started) {
      return new Promise((resolve, reject) => {
        this.outputQueue = this.outputQueue.concat({eventsBucket, resolve, reject})
      })
    }
    console.log("WRITE EVENTS", eventsBucket)
    return this.writeBucket(eventsBucket)
  }

  createEventsBucket(events, origin) {
    let out = {}
    for(let event of events) {
      for(let key in event) out[key] = event[key]
    }
    out.events = events
    out.timestamp = new Date
    out.origin = origin
    out.type = "bucket"
    return out
  }

  writeBucket(eventsBucket, retry) {

    function handleError(error) {
      console.error("RethinkDB error", error)
      if(!retry) retry = 0
      if(retry >= 3) {
        let emptyBucket = { /// Empty bucket to maintain sequence
          id: eventsBucket.id,
          events: [],
          error: error.message || error,
          failedBucket: eventsBucket,
          timestamp: eventsBucket.timestamp,
          origin: eventsBucket.origin,
          type: "bucket"
        }
        console.error("BUCKET write failed", eventsBucket, error)
        console.error("WRITING empty bucket")
        return r.table(this.tableName).insert(emptyBucket, { returnChanges: true }).run(this.service.db).then(
          inserted => {
            if(error instanceof Error) throw error
            throw new Error(error.message || error)
          }
        ).catch(
          error => {
            console.error("DATABASE failure",error)
            console.error("Exiting")
            process.exit(1)
          }
        )

      }
      let retryDelay = 100 * Math.pow(2, retry)
      console.error(`Replaying request in ${retryDelay} ms`)
      return new Promise((resolve, reject) => setTimeout(() => {
        this.writeBucket(eventsBucket, retry + 1).then(resolve).catch(reject)
      }, retryDelay))
    }

    console.log("SAVE BUCKET", eventsBucket)
    if(!retry) eventsBucket.id = ++this.lastInsertedId
    return r.table(this.tableName).insert(eventsBucket, { returnChanges: true }).run(this.service.db).then(
      result => {
        if(result.errors > 0) {
          if(result.first_error.match(/^Duplicate primary key/)) {
            console.log("DUPLICATED EVENT PRIMARY KEY", eventsBucket.id)
            return new Promise((resolve, reject) =>
                setTimeout(() => this.writeBucket(eventsBucket).then(resolve).catch(reject), 50)
            )
          } else {
            return handleError(result.first_error)
          }
        }
        return true
      }
    ).catch(
      error => handleError(error)
    )
  }

  start() {
    return r.tableCreate(this.tableName).run(this.service.db).then(
      result => this.startWithTable()
    ).catch(
      error => this.startWithTable()
    )
  }
  async startWithTable() {
    const startTime = Date.now()
    const cursor = await r.table(this.tableName).orderBy({ index: r.desc('id') }).limit(1)('id')
        .run(this.service.db)
    let [id] = await cursor.toArray()
    if(id === undefined) id = 0
    console.log("LAST ID", id)
    this.lastInsertedId = id
    r.table(this.tableName).changes({ includeTypes: 'add' })('new_val')('id')
        .run(this.service.db, (err, cursor) => {
          cursor.each(id => { if(id > this.lastInsertedId) this.lastInsertedId = id });
        })
    this.started = true
    console.log(`EVENT WRITER ${this.tabeName} STARTED IN ${Date.now() - startTime} ms`)
    let out = this.outputQueue
    for(let {eventsBucket,resolve,reject} of out) {
      this.writeBucket(eventsBucket).then(
        result => resolve && resolve(result)
      ).catch(
        error => reject && reject(result)
      )
    }
    return true
  }
  
}


module.exports = EventWriter
