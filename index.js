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
      let changesStream = await connection.run(
          r.table( svc + '_commands' ).get(commandId).changes({ includeInitial: true  })
      )
      changesStream.each( (err, result) => {
        if(err) {
          changesStream.close()
          if(connection.handleDisconnectError(err)) return readResult()
          reject(err)
          return false
        }
        let val = result.new_val
        if(val.state == "done") {
          resolve(val.result)
          changesStream.close()
          return false
        }
        if(val.state == "failed") {
          reject(val.error)
          changesStream.close()
          return false
        }
      })
    }
    readResult()
  })

}