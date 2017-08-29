const r = require('rethinkdb')
const EventSource = require('./EventSource.js')
const CommandSource = require('./CommandSource.js')
const EventWriter = require('./EventWriter.js')
const ExecutionQueue = require('./ExecutionQueue.js')

class Service {
  constructor(config) {
    this.db = undefined
    this.serviceName = config.serviceName
    if(!this.serviceName) throw "service name undefined"

    this.started = false

    this.commandSources = new Map()
    this.eventSources = new Map()
    this.installRoutines = []
    this.startRoutines = []

    this.eventWriters = new Map()

    this.queues = new Map()

    r.connect({
      host: process.env.DB_HOST,
      port: process.env.DB_PORT,
      db: process.env.DB_NAME || this.serviceName,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      timeout: process.env.DB_TIMEOUT,
    }).then(
      conn => {
        this.db = conn
        this.start();
      }
    )
  }
  
  install() {
    let installPromises = this.installRoutines.map(installCallback => installCallback())
    return Promise.all(installPromises)
  }

  start() {
    if(process.env.INSTALL) {
      this.install().then(
        installed => this.db.close().then(
          closed => {
            console.log("all installed")
            process.exit(0)
          }
        )
      ).catch(
        error => {
          console.log("installation failed")
          process.exit(1)
        }
      )
      return;
    }
    for(let startCallback of this.startRoutines) {
      startCallback()
    }
    var writerPromises = []
    for(let writer of this.eventWriters.values()) {
      writerPromises.push(writer.start())
    }
    Promise.all(writerPromises).then(() => {
      for(let commandSource of this.commandSources.values()) {
        commandSource.start()
      }
      for(let eventSource of this.eventSources.values()) {
        eventSource.start()
      }
      this.started = true
    })
  }
  onInstall(cb) {
    this.installRoutines.push(cb)
  }
  onStart(cb) {
    this.startRoutines.push(cb)
  }
  registerEventListeners(listeners) {
    const sourceName = listeners.source || this.serviceName
    const eventSource = this.eventSources.get(sourceName) || new EventSource(this, sourceName)
    this.eventSources.set(sourceName, eventSource)
    if(this.started && !eventSource.started) eventSource.start()

    const transformMethod = (method, transform) => {
      return (event) => transform(method, event)
    }

    for(let methodName in listeners) {
      if(!listeners.hasOwnProperty(methodName)) continue;
      let method = listeners[methodName]
      if(typeof method != 'function') continue;

      if(listeners.queuedBy) method = transformMethod(method, (m, ev) => {
        const key = listeners.queuedBy + "_" + ev[listeners.queuedBy]
        let queue = this.queues.get(key)
        if(!queue) {
          queue = new ExecutionQueue(this, key)
          this.queues.set(key, queue)
        }
        queue.queue(() => {
          //console.log("EVENT QUEUED IN QUEUE", key, ":\n", ev)
          m(ev)
        } )
      })

      eventSource.registerEventListener(methodName, method)
    }
    if(listeners.ignoreNotHandled) eventSource.registerEventListener(()=>{})
  }

  registerCommands(executors) {
    const sourceName = executors.source || this.serviceName
    const commandSource = this.commandSources.get(sourceName) || new CommandSource(this, sourceName)
    this.commandSources.set(sourceName, commandSource)
    if(this.started && !commandSource.started) commandSource.start()
    for(let methodName in executors) {
      if(!executors.hasOwnProperty(methodName)) continue;
      let method = executors[methodName]
      if(typeof method != 'function') continue;
      commandSource.registerCommandExecutor(methodName, method)
    }
  }

  emitEvents(listName, events, commandId) {
    let eventWriter = this.eventWriters.get(listName)
    if(!eventWriter) {
      eventWriter = new EventWriter(this, listName)
      this.eventWriters.set(listName, eventWriter)
    }
    if(this.started && !eventWriter.started) eventWriter.start()
    eventWriter.writeEvents(events, commandId)
  }

  error(message) {
    return new Error(message)
  }
  
  call(serviceName, command, byCommand) {
    let cmd = JSON.parse(JSON.stringify(command))
    cmd.calledBy = { service: this.serviceName }
    cmd.state = "new"
    cmd.timestamp = new Date()
    if(byCommand) cmd.calledBy.command = byCommand
    
    return r.table(serviceName + '_commands').insert(cmd).run(this.db)
      .then( result => result.generated_keys[0])
      .then( commandId => new Promise((resolve, reject) => {
        r.table(serviceName + '_commands').get(commandId).changes({ includeInitial: true  }).run(this.db,
          (err, cursor) => {
            cursor.each((err, result) => {
              console.log("CURSOR OUT", err, result)
              if(err) {
                cursor.close();
                reject("cursorError: "+ err)
                return false
              }
              let val = result.new_val
              if(val.state == "done") {
                cursor.close()
                resolve(val.result)
                return false
              }
              if(val.state == "failed") {
                cursor.close()
                reject(val.error)
                return false
              }
            })
          }
        )
      }))
  }
}

module.exports = Service
Service.EventSource = EventSource
Service.CommandSource = CommandSource
Service.EventWriter = EventWriter
Service.ExecutionQueue = ExecutionQueue
