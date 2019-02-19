const r = require('rethinkdb')
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
  let result = await r.table( svc + "_commands" ).insert(cmd).run(connection)
  let commandId = result.generated_keys[0]
  let changesStream = await r.table( svc + '_commands' ).get(commandId)
      .changes({ includeInitial: true  }).run(connection)
  return new Promise( (resolve, reject) => {
    changesStream.each( (err, result) => {
      if(err) {
        changesStream.close();
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
  })
}