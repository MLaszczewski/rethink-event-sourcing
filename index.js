const r = require('rethinkdb-reconnect')
const Service = require("./lib/Service.js")

module.exports = (config) => {
  return new Service(config)
}

module.exports.Service = Service

module.exports.command = async function(connection, service, command, parameters) {
  let cmd
  if(parameters) {
    cmd = parameters
    cmd.type = command
  } else {
    cmd = command
  }
  cmd.state = "new"
  cmd.timestamp = new Date()

  let svc = service.definition ? service.definition.name : service.name || service
  let result = await connection.run(
      r.table( svc + "_commands" ).insert(cmd, { conflict: (id,o,n) => n.merge(o) })
  )
  let commandId = cmd.id || result.generated_keys[0]

  return new Promise( (resolve, reject) => {
    const readResult = async () => {
      let changeStream = await connection.run(
          r.table( svc + '_commands' ).get(commandId).changes({ includeInitial: true  })
      )
      changeStream.each( (err, result) => {
        if(err) {
          changeStream.close()
          if(connection.handleDisconnectError(err)) return readResult()
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

}