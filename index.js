
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
  let result = await r.table( service + "_commands" ).insert(cmd).run(command)
  let commandId = result.generated_keys[0]
  let changeStream = await r.table( service + '_commands' ).get(commandId).changes({ includeInitial: true  }).run(conn)
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