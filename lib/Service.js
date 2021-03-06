const r = require('rethinkdb-reconnect')
const EventSource = require('./EventSource.js')
const CommandSource = require('./CommandSource.js')
const TriggerSource = require('./TriggerSource.js')
const EventWriter = require('./EventWriter.js')
const ExecutionQueue = require('./ExecutionQueue.js')

class Service {
  constructor(config) {
    console.log("SERVICE CONSTRUCTOR")
    this.db = undefined
    this.serviceName = config.serviceName
    if(!this.serviceName) throw "service name undefined"

    this.started = false

    this.commandSources = new Map()
    this.triggerSources = new Map()
    this.eventSources = new Map()
    this.installRoutines = []
    this.startRoutines = []

    this.eventWriters = new Map()

    this.queues = new Map()

    if(!config.noAutostart) {
      setTimeout(() => {
        this.db = r.autoConnection()
        this.start()
      }, 0)
    }
    
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
          console.error("installation failed: ", error)
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
      for(let triggerSource of this.triggerSources.values()) {
        triggerSource.start()
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

      const queuedBy = method.queuedBy || listeners.queuedBy

      if(queuedBy) method = transformMethod(method, (m, ev) => {
        const key = 'event_' + queuedBy + "_" + ev[queuedBy]
        let queue = this.queues.get(key)
        if(!queue) {
          queue = new ExecutionQueue(this, key)
          this.queues.set(key, queue)
        }
        queue.queue(() => {
          console.log("EVENT QUEUED IN QUEUE", key, ":\n", ev)
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
    let promises = []
    if(this.started && !commandSource.started) promises.push(commandSource.start())
    for(let methodName in executors) {
      if(!executors.hasOwnProperty(methodName)) continue;
      let method = executors[methodName]
      if(typeof method != 'function') continue;

      const queuedBy = method.queuedBy || executors.queuedBy

      if(queuedBy) {
        let oldMethod = method
        method = (command, emit) => new Promise((resolve, reject) => {
          const key = 'command_' + queuedBy + "_" + command[queuedBy]
          let queue = this.queues.get(key)
          if(!queue) {
            queue = new ExecutionQueue(this, key)
            this.queues.set(key, queue)
          }
          queue.queue(() => {
            console.log("COMMAND QUEUED IN QUEUE", key, ":\n", ev)
            oldMethod(command, emit).then(resolve).catch(reject)
          } )
        })
      }

      commandSource.registerCommandExecutor(methodName, method)
    }
    return Promise.all(promises)
  }

  async registerTriggers(triggers) {
    await this.db.run(r.tableCreate('triggers')).catch(err => 'ok')
    const sourceName = triggers.source || this.serviceName
    const triggerSource = this.triggerSources.get(sourceName) || new TriggerSource(this, sourceName)
    this.triggerSources.set(sourceName, triggerSource)
    let promises = []
    if(this.started && !triggerSource.started) promises.push(triggerSource.start())
    for(let methodName in triggers) {
      if(!triggers.hasOwnProperty(methodName)) continue;
      let method = triggers[methodName]
      if(typeof method !== 'function') continue;

      const queuedBy = method.queuedBy || triggers.queuedBy

      if(queuedBy) {
        let oldMethod = method
        method = (command, emit) => new Promise((resolve, reject) => {
          const key = 'command_' + queuedBy + "_" + command[queuedBy]
          let queue = this.queues.get(key)
          if(!queue) {
            queue = new ExecutionQueue(this, key)
            this.queues.set(key, queue)
          }
          queue.queue(() => {
            console.log("TRIGGER QUEUED IN QUEUE", key, ":\n", ev)
            oldMethod(command, emit).then(resolve).catch(reject)
          } )
        })
      }

      triggerSource.registerTriggerExecutor(methodName, method)
      promises.push(
        this.db.run(
          r.table("triggers").insert({ id: methodName, list:[sourceName] }, {
            conflict: (id,ol,ne) => ol.merge({ list: ol('list').setUnion(ne('list')) })
          })
        ).catch(err => console.error(err))
      )
    }
    return Promise.all(promises)
  }

  triggerService(serviceName, command, origin) {
    let cmd = JSON.parse(JSON.stringify(command))
    cmd.origin = {}
    cmd.state = "new"
    cmd.timestamp = new Date()
    if (origin) cmd.origin = origin
    cmd.origin.service = this.serviceName

    console.log("TRIG CMD", cmd)

    return this.db.run(r.table(serviceName + '_triggers').insert(cmd))
        .then(result => result.generated_keys[0])
        .then(triggerId => new Promise((resolve, reject) => {
              const readResult = async () => {
                let changeStream = await this.db.run(
                    r.table(serviceName + '_triggers').get(triggerId).changes({includeInitial: true})
                )
                changeStream.each((err, result) => {
                  if (err) {
                    changeStream.close()
                    if (this.db.handleDisconnectError(err)) return readResult()
                    reject(err)
                    return false
                  }
                  let val = result.new_val
                  if (val.state == "done") {
                    resolve(val.result)
                    changeStream.close()
                    return false
                  }
                  if (val.state == "failed") {
                    reject(val.error)
                    changeStream.close()
                    return false
                  }
                })
              }
              readResult()
            })
        )
  }

  trigger(command, byCommand) {
    return this.db.run(r.table('triggers').get(command.type)).then(
      triggerDesc => {
        if(!triggerDesc) return;
        let list = triggerDesc.list || []
        let promises = list.map(sn => this.triggerService(sn, command, byCommand))
        return Promise.all(promises)
      }
    )
  }

  emitEvents(listName, events, origin) {
    let eventWriter = this.eventWriters.get(listName)
    if(!eventWriter) {
      eventWriter = new EventWriter(this, listName)
      this.eventWriters.set(listName, eventWriter)
    }
    if(this.started && !eventWriter.started) eventWriter.start()
    return eventWriter.writeEvents(events, origin)
  }

  error(message) {
    return new Error(message)
  }
  
  call(serviceName, command, origin) {
    let cmd = JSON.parse(JSON.stringify(command))
    cmd.origin = {}
    cmd.state = "new"
    cmd.timestamp = new Date()
    if(origin) cmd.origin = origin
    cmd.origin.service = this.serviceName
    
    return this.db.run(r.table(serviceName + '_commands').insert(cmd))
      .then( result => result.generated_keys[0])
      .then( commandId => new Promise( (resolve, reject) => {
          const readResult = async () => {
            let changeStream = await this.db.run(
                r.table( serviceName + '_commands' ).get(commandId).changes({ includeInitial: true  })
            )
            changeStream.each( (err, result) => {
              if(err) {
                changeStream.close()
                if(this.db.handleDisconnectError(err)) return readResult()
                reject(err)
                return false
              }
              let val = result.new_val
              if(val.state == "done") {
                resolve(val.result)
                changeStream.close()
                return false
              }
              if(val.state == "failed") {
                reject(val.error)
                changeStream.close()
                return false
              }
            })
          }
          readResult()
        })
      )

  }
}

module.exports = Service
Service.EventSource = EventSource
Service.CommandSource = CommandSource
Service.EventWriter = EventWriter
Service.ExecutionQueue = ExecutionQueue
