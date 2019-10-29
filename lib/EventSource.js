const r = require('rethinkdb-reconnect')

const holeRepairDelay = 1000

class EventSource {
  constructor(service, name) {
    this.name = name
    this.service = service
    this.started = false

    this.tableName = name + "_events"
    this.listenerName = this.service.serviceName+"_"+this.name

    this.typedListeners = new Map()
    this.listeners = []

    this.lastReadedId = 0

    this.waitingEvents = []

    this.lastEventProcessingTime = Date.now()
  }

  registerEventListener(eventType, callback) {
    if(!callback) {
      callback = eventType
      this.listeners.push(callback)
    } else {
      let listeners = this.typedListeners.get(eventType) || []
      listeners.push(callback)
      this.typedListeners.set(eventType, listeners)
    }
  }

  start() {
    let db = this.service.db
    return db.run(r.tableCreate(this.tableName)).catch(err => 'ok')
        .then(()=>db.run(r.tableCreate("eventListeners")).catch(err => 'ok'))
        .then(()=>db.run(r.table(this.tableName).indexCreate("timestamp")).catch(err => 'ok'))
        .then(()=>db.run(r.table(this.tableName).indexCreate("origin")).catch(err => 'ok'))
        .then(result => this.startWithTable())
  }
  
  startWithTable() {
    let eventListenerPromise = this.service.db.run(
        r.table("eventListeners").get(this.listenerName)
    )
    let lastEventPromise = this.service.db.run(
        r.table(this.tableName).orderBy(r.desc("id")).limit(1)
    ).then(cur => cur.toArray()).then(arr => arr[0])
    return Promise.all([eventListenerPromise, lastEventPromise]).then(
      ([eventListenerRow, lastEventRow]) => {
        if(eventListenerRow) {
          this.lastReadedId = eventListenerRow.lastReadedId || 0
          if(lastEventRow && lastEventRow.id < this.lastReadedId) this.lastReadedId = lastEventRow.id
        } else {
          this.lastReadedId = 0
        }
        const req = r.table(this.tableName).filter(r.row('id').gt(this.lastReadedId)).changes({ includeInitial: true })
        const read = () => {
          this.service.db.run(req, { timeout: Infinity }).then(
              changeStream => {
                changeStream.each(
                    (err, result) => {
                      if(err) {
                        console.error(" DB ERROR ", err)
                        changesStream.close()
                        return read()
                      }
                      if(!result.old_val) {
                        this.handleEvents(result.new_val)
                      }
                    }
                )
              }
          )
        }
        read()
        this.started = true
        return true
      }
    )
  }
  handleEvents(eventsBucket) {
    //console.log("HANDLE EVENTS BUCKET", eventsBucket)
    if(eventsBucket.id == this.lastReadedId + 1) {
      this.processEvents(eventsBucket)

      this.waitingEvents.sort((a, b) => a.id - b.id)
      while(this.waitingEvents.length > 0 && this.waitingEvents[0].id == this.lastReadedId + 1) {
        this.processEvents(this.waitingEvents.shift())
      }

      return
    }
    //console.log("QUEUE EVENTS Bucket", eventsBucket, eventsBucket.id, this.lastReadedId)
    this.waitingEvents.push(eventsBucket)
    if(this.waitingEvents.length == 1) setTimeout(() => this.fillHole(), holeRepairDelay + 200)
  }
  fillHole() {
    if(this.waitingEvents.length == 0) return;
    this.waitingEvents.sort((a, b) => a.id - b.id)
    if(Date.now() - this.lastEventProcessingTime > holeRepairDelay && this.waitingEvents.length > 0) {
      console.error(`PREFORM HOLE REPAIR:  lastReadedId=${this.lastReadedId} nextEventId=${this.waitingEvents[0].id}`)
      let emptyBucket = { /// Empty bucket to maintain sequence
        id: this.lastReadedId + 1,
        events: [],
        error: "holeDetected",
        timestamp: new Date,
        type: "bucket"
      }
      this.service.db.run(r.table(this.tableName).insert(emptyBucket, { returnChanges: true })).then(
          result => {
            if(result.errors > 0) {
              if(result.first_error.match(/^Duplicate primary key/)) {
                console.error("HOLE FILLED BY ANOTHER PROCESS")
                return;
              }
              console.error("DATABASE FAILURE", result.first_error)
              process.exit(1)
              return;
            }
            console.error("HOLE FILLED")
          }
      ).catch(
          error => {
            console.error("DATABASE FAILURE", error)
            process.exit(1)
          }
      )
    }
    if(this.waitingEvents.length > 0 && this.waitingEvents[0].id > this.lastReadedId+1) { /// Still there are holes to fill
      setTimeout(() => this.fillHole(), 50)
    }
  }
  processEvents(eventsBucket) {
    this.lastEventProcessingTime = Date.now()
    //console.log("PROCESS EVENTS BUCKET", eventsBucket)
    if(!eventsBucket.events) { // Single event
      this.processEvent(eventsBucket)
    } else { // Bucket
      for (let event of eventsBucket.events) this.processEvent(event)
    }
    if(eventsBucket.id > this.lastReadedId) {
      this.lastReadedId = eventsBucket.id
      this.service.db.run(
          r.table("eventListeners").insert({
            id: this.listenerName,
            lastReadedId: this.lastReadedId
          }, {
            conflict: "update"
          })
      )
    }
  }
  processEvent(event) {
    console.log("PROCESS EVENT", event)
    let handled = false
    const typedListeners = this.typedListeners.get(event.type)
    if(typedListeners) {
      for(let typedListener of typedListeners) {
        handled = handled || (typedListener(event, handled) !== false)
      }
    }
    for(let listener of this.listeners) {
      handled = handled || (listener(event, handled) !== false)
    }
    if(!handled) {
      console.error("Unhandled event", event.type, ":", event)
    }
  }
}


module.exports = EventSource
