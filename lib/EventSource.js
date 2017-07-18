const r = require('rethinkdb')

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
    r.tableCreate(this.tableName).run(this.service.db).then(
      result => this.startWithTable()
    ).catch(
      error => this.startWithTable()
    )
  }
  
  startWithTable() {
    return r.table("eventListeners").get(this.listenerName).run(this.service.db).then(
      result => {
        if(result) {
          this.lastReadedId = result.lastReadedId || 0
        } else {
          this.lastReadedId = 0
        }
        const req = r.table(this.tableName).filter(r.row('id').gt(this.lastReadedId)).changes({ includeInitial: true })
        req.run(this.service.db).then(
          results => {
            results.each(
              (err, result) => {
                if(err) console.error(" DB ERROR ", err)
                if(!result.old_val) {
                  this.handleEvents(result.new_val)
                }
              }
            )
          }
        )
        this.started = true
        return true
      }
    )
  }
  handleEvents(eventsBucket) {
    //console.log("HANDLE EVENTS BUCKET", eventsBucket)
    if(eventsBucket.id == this.lastReadedId + 1) {
      this.processEvents(eventsBucket)

      this.waitingEvents.sort((a, b) => a.id > b.id)
      while(this.waitingEvents.length > 0 && this.waitingEvents[0].id == this.lastReadedId + 1) {
        this.processEvents(this.waitingEvents.shift())
      }

      return
    }
    //console.log("QUEUE EVENTS Bucket", eventsBucket, eventsBucket.id, this.lastReadedId)
    this.waitingEvents.push(eventsBucket)
  }
  processEvents(eventsBucket) {
    //console.log("PROCESS EVENTS BUCKET", eventsBucket)
    if(!eventsBucket.events) { // Single event
      this.processEvent(eventsBucket)
    } else { // Bucket
      for (let event of eventsBucket.events) this.processEvent(event)
    }
    if(eventsBucket.id > this.lastReadedId) {
      this.lastReadedId = eventsBucket.id
      r.table("eventListeners").insert({
        id: this.listenerName,
        lastReadedId: this.lastReadedId
      }, {
        conflict: "update"
      }).run(this.service.db)
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
