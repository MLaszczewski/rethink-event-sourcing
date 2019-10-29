const r = require('rethinkdb-reconnect')

class TriggerSource {
  constructor(service, name) {
    this.name = name
    this.service = service
    this.started = false

    this.tableName = name + "_triggers"

    this.triggerExecutors = new Map()
  }

  registerTriggerExecutor(methodName, method) {
    this.triggerExecutors.set(methodName, method)
  }

  handleTriggerResult(trigger, result) {
    //console.log("HANDLE TRIGGER RESULT", result)
    if (result && result.then) {
      return result.then(
        result => this.handleTriggerResult(trigger, result)
      ).catch(
        error => this.handleTriggerError(trigger, error)
      )
    }
    this.service.db.run(
      r.table(this.tableName).get(trigger.id).update({ state: "done", result: result === undefined ? null : result })
    )
  }
  handleTriggerError(trigger, error) {
    console.log("TRIGGER ",trigger, "ERROR:")
    console.error(error)
    if(error && error.message) error = error.message
    this.service.db.run(
      r.table(this.tableName).get(trigger.id)
          .update({ state: "failed", error: error === undefined ? null : (error.stack || error) })
    )
  }
  handleTrigger(trigger) {
    console.log("HANDLE TRIGGER", trigger)
    const executor = this.triggerExecutors.get(trigger.type)
    if(!executor) {
      console.error("Unhandled trigger", trigger.type, ":", trigger)
      return;
    }
    try {
      let result = executor(trigger, (to, events) => 
        this.service.emitEvents(events ? to : this.service.serviceName, events ? events : to,
          { type: "trigger", id: trigger.id, triggerType: trigger.type })
      )
      this.handleTriggerResult(trigger, result )
    } catch(error) {
      this.handleTriggerError(trigger, error )
    }
  }

  start() {
    return this.service.db.run(r.tableCreate(this.tableName)).then(
      result => this.startWithTable()
    ).catch(
      error => this.startWithTable()
    )
  }

  startWithTable() {
    const req = r.table(this.tableName).filter(r.row('state').eq("new")).changes({ includeInitial: true })
    const read = () => {
      this.service.db.run(req, {timeout: Infinity}).then(
          changesStream => {
            // TODO: Sequential reading with pool
            changesStream.each((err, result) => {
              if(err) {
                console.error(" DB ERROR ", err)
                changesStream.close()
                return read()
              }
              if(!result.old_val && result.new_val.state == "new") {
                this.handleTrigger(result.new_val)
              }
            })
          }
      )
    }
    read()
    this.started = true
    return true
  }
}



module.exports = TriggerSource
