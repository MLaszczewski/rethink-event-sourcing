const r = require.main.rethinkdb || require('rethinkdb')

class CommandSource {
  constructor(service, name) {
    this.name = name
    this.service = service
    this.started = false

    this.tableName = name + "_triggers"

    this.triggerExecutors = new Map()
  }

  registerCommandExecutor(methodName, method) {
    this.triggerExecutors.set(methodName, method)
  }

  handleCommandResult(trigger, result) {
    //console.log("HANDLE COMMAND RESULT", result)
    if (result && result.then) {
      return result.then(
        result => this.handleCommandResult(trigger, result)
      ).catch(
        error => this.handleCommandError(trigger, error)
      )
    }
    r.table(this.tableName).get(trigger.id).update({ state: "done", result: result === undefined ? null : result })
      .run(this.service.db)
  }
  handleCommandError(trigger, error) {
    console.log("COMMAND ",trigger, "ERROR:")
    console.error(error)
    if(error && error.message) error = error.message
    r.table(this.tableName).get(trigger.id).update({ state: "failed", error: error === undefined ? null : error })
      .run(this.service.db)
  }
  handleCommand(trigger) {
    console.log("HANDLE COMMAND", trigger)
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
      this.handleCommandResult(trigger, result )
    } catch(error) {
      this.handleCommandError(trigger, error )
    }
  }

  start() {
    return r.tableCreate(this.tableName).run(this.service.db).then(
      result => this.startWithTable()
    ).catch(
      error => this.startWithTable()
    )
  }

  startWithTable() {
    const req = r.table(this.tableName).filter(r.row('state').eq("new")).changes({ includeInitial: true })
    req.run(this.service.db).then(
      results => {
        // TODO: Sequential reading with pool
        results.each((err, result) => {
          if(err) console.error(" DB ERROR ", err)
          if(!result.old_val && result.new_val.state == "new") {
            this.handleCommand(result.new_val)
          }
        })
      }
    )
    this.started = true
    return true
  }
}



module.exports = CommandSource
