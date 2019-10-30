const r = require('rethinkdb-reconnect')

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

    const handleError = (error) => {
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
        return this.service.db.run(
            r.table(this.tableName).insert(emptyBucket, { returnChanges: true })
        ).then(
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
    return this.service.db.run(
        r.table(this.tableName).insert(eventsBucket, { returnChanges: true })
    ).then(
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
    return this.service.db.run(r.tableCreate(this.tableName)).then(
      result => this.startWithTable()
    ).catch(
      error => this.startWithTable()
    )
  }
  startIdObserver() {
    const read = () => {
      this.service.db.run(
          r.table(this.tableName).changes({ includeTypes: 'add' })('new_val')('id'),
          { timeout: Infinity }
      ).then(changeStream => {
        changeStream.each((err, id) => {
          if(err) {
            console.error(" DB ERROR ", err)
            changeStream.close()
            return read()
          }
          if(id > this.lastInsertedId) this.lastInsertedId = id
        })
      })
    }
  }
  async startWithTable() {
    const startTime = Date.now()
    const cursor = await this.service.db.run(
        r.table(this.tableName).orderBy({ index: r.desc('id') }).limit(1)('id')
    )
    let [id] = await cursor.toArray()
    if(id === undefined) id = 0
    console.log("LAST ID", id)
    this.lastInsertedId = id
    this.startIdObserver()
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
