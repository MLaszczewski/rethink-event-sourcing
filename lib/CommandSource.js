const r = require('rethinkdb')

class CommandSource {
  constructor(service, name) {
    this.name = name
    this.service = service
    this.started = false

    this.tableName = name + "_commands"

    this.commandExecutors = new Map()
  }

  registerCommandExecutor(methodName, method) {
    this.commandExecutors.set(methodName, method)
  }

  handleCommandResult(command, result) {
    //console.log("HANDLE COMMAND RESULT", result)
    if (result && result.then) {
      return result.then(
        result => this.handleCommandResult(command, result)
      ).catch(
        error => this.handleCommandError(command, error)
      )
    }
    r.table(this.tableName).get(command.id).update({ state: "done", result: result === undefined ? null : result })
      .run(this.service.db)
  }
  handleCommandError(command, error) {
    console.log("COMMAND ",command, "ERROR:")
    console.error(error)
    if(error && error.message) error = error.message
    r.table(this.tableName).get(command.id).update({ state: "failed", error: error === undefined ? null : error })
      .run(this.service.db)
  }
  handleCommand(command) {
    console.log("HANDLE COMMAND", command)
    const executor = this.commandExecutors.get(command.type)
    if(!executor) {
      console.error("Unhandled command", command.type, ":", command)
      return;
    }
    try {
      let result = executor(command, (to, events) => 
        this.service.emitEvents(events ? to : this.service.serviceName, events ? events : to,
          { type:"command", id: command.id, type: command.type })
      )
      this.handleCommandResult(command, result )
    } catch(error) {
      this.handleCommandError(command, error )
    }
  }

  start() {
    let db = this.service.db
    return r.tableCreate(this.tableName).run(db).catch(err => 'ok')
      .then(()=>r.table(this.tableName).indexCreate("timestamp").run(db).catch(err => 'ok'))
      .then(()=>r.table(this.tableName).indexCreate("ip").run(db).catch(err => 'ok'))
      .then(result => this.startWithTable())
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
