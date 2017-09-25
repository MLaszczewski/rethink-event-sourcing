const r = require('rethinkdb')

class EventWriter {
  constructor(service, name) {
    this.name = name
    this.service = service
    this.started = false

    this.tableName = this.name + "_events"

    this.lastInsertedId = 0

    this.outputQueue = []
  }

  writeEvents(events, commandId) {
    let eventsBucket
    if(events.length == 1) {
      eventsBucket = events[0]
      eventsBucket.timestamp = new Date()
      eventsBucket.commandId = commandId
    } else {
      eventsBucket = this.createEventsBucket(events, commandId)
    }
    if(!this.started) {
      return new Promise((resolve, reject) => {
        this.outputQueue = this.outputQueue.concat({eventsBucket, resolve, reject})
      })
    }
    console.log("WRITE EVENTS", eventsBucket)
    return this.writeBucket(eventsBucket)
  }

  createEventsBucket(events, commandId) {
    let out = {}
    for(let event of events) {
      for(let key in event) out[key] = event[key]
    }
    out.events = events
    out.timestamp = new Date
    out.commandId = commandId
    out.type = "bucket"
    return out
  }

  writeBucket(eventsBucket) {
    eventsBucket.id = ++this.lastInsertedId
    return r.table(this.tableName).insert(eventsBucket, { returnChanges: true }).run(this.service.db).then(
      result => {
        if(result.errors > 0) return new Promise(
          (resolve, reject) => setTimeout(() => this.writeBucket(eventsBucket).then(resolve).catch(reject), 50)
        )
        return true
      }
    ).catch(
      error => {
        console.error("RethinkDB error", error)
        console.error("Replaying request in 500ms")
        setTimeout(() => this.writeBucket(eventsBucket), 500)
      }
    )
  }

  start() {
    return r.tableCreate(this.tableName).run(this.service.db).then(
      result => this.startWithTable()
    ).catch(
      error => this.startWithTable()
    )
  }
  startWithTable() {
    return new Promise((resolve, reject) => {
      r.table(this.tableName).orderBy(r.desc(r.row('id'))).limit(1)('id').run(this.service.db).then(
        ([ id ]) => {
          if(id === undefined) id = 0
          console.log("LAST ID", id)
          this.lastInsertedId = id
          r.table(this.tableName).changes({ includeTypes: 'add' })('new_val')('id').run(this.service.db, (err, cursor) => {
            cursor.each(id => { if(id > this.lastInsertedId) this.lastInsertedId = id });
          })
          this.started = true
          let out = this.outputQueue
          for(let {eventsBucket,resolve,reject} of out) {
            this.writeBucket(eventsBucket).then(
              result => resolve && resolve(result)
            ).catch(
              error => reject && reject(result)
            )
          }
          resolve(true)
        }
      ).catch(reject)
    })
  }
  
}


module.exports = EventWriter
