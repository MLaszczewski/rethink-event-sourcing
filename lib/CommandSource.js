const r = require('rethinkdb-reconnect')

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
    this.service.db.run(
        r.table(this.tableName).get(command.id).update({ state: "done", result: result === undefined ? null : result })
    )
  }
  handleCommandError(command, error) {
    console.log("COMMAND ",command, "ERROR:")
    console.error(error)
    if(error && error.message) error = error.message
    this.service.db.run(
        r.table(this.tableName).get(command.id).update({ state: "failed", error: error === undefined ? null : error })
    )
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
          { type:"command", id: command.id, commandType: command.type })
      )
      this.handleCommandResult(command, result )
    } catch(error) {
      this.handleCommandError(command, error )
    }
  }

  start() {
    let db = this.service.db
    return this.service.db.run(
        r.tableCreate(this.tableName)
    ).catch(err => 'ok')
      .then(()=> this.service.db.run(
          r.table(this.tableName).indexCreate("timestamp")
      ).catch(err => 'ok'))
      .then(()=> this.service.db.run(
          r.table(this.tableName).indexCreate("sessionId")
      ).catch(err => 'ok'))
      .then(()=> this.service.db.run(
          r.table(this.tableName).indexCreate("ip")
      ).catch(err => 'ok'))
      .then(result => this.startWithTable())
  }

  startWithTable() {
    const req = r.table(this.tableName).filter(r.row('state').eq("new")).changes({ includeInitial: true })
    const read = () => {
      this.service.db.run(req, {timeout: Infinity}).then(
        changeStream => {
          // TODO: Sequential reading with pool
          changeStream.each((err, result) => {
            if(err) {
              console.error(" DB ERROR ", err)
              changeStream.close()
              return read()
            }
            if(!result.old_val && result.new_val.state == "new") {
              this.handleCommand(result.new_val)
            }
          })
        }
      ).catch(err => {
        console.error(" DB ERROR ", err)
        changeStream.close()
        return read()
      })
    }
    read()
    this.started = true
    return true
  }
}



module.exports = CommandSource
