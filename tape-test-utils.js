
module.exports.connectToDatabase = function(t,r,cb) {
  t.test('Connect to database', t => {
    t.plan(1)
    r.connect({
      host: process.env.DB_HOST,
      port: process.env.DB_PORT,
      db: process.env.DB_NAME,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      timeout: process.env.DB_TIMEOUT,
    }).then(
      c => {
        module.exports.connection = c
        cb(c)
        t.pass("connected")
      }
    ).catch(
      e => t.fail("connection error", e)
    )
  })
}

module.exports.runCommand = function(t, r, cmd, cb) {
  let commandId
  t.test("Run command " + cmd.type, t => {
    t.plan(2)
    t.test('Push command', t => {
      t.plan(2)
      cmd.state = "new",
        r.table('newsletter_commands').insert(cmd).run(module.exports.connection).then( result => {
          t.equal(result.inserted, 1, "one command inserted")
          commandId = result.generated_keys[0]
          cb(commandId)
          t.equal(result.generated_keys.length, 1, "one key generated")
        })
    })

    t.test('Wait for command done', t => {
      t.plan(2)
      r.table('newsletter_commands').get(commandId).changes({ includeInitial: true  }).run(module.exports.connection,
        (err, cursor) => {
          t.equal(err, null, "no error")
          cursor.each((err, result) => {
            if(err) {
              cursor.close();
              t.fail("cursor error: "+ err)
              return false
            }
            let val = result.new_val
            if(val.state == "done") {
              t.pass("command succeed with result: "+ JSON.stringify(val.result))
              cursor.close()
              return false
            }
            if(val.state == "failed") {
              t.fail("command failed with error: "+ JSON.stringify(val.error))
              cursor.close()
              return false
            }
          })
        }
      )
    })
  })

}

module.exports.getGeneratedEvents = function(r, table, commandId, cb) {
  r.table(table + '_events').filter(r.row('commandId').eq(commandId)).run(module.exports.connection).then(
    results => results.toArray().then(
      eventBuckets => {
        cb(Array.prototype.concat.apply([], eventBuckets.map(bucket => bucket.events)))
      }
    )
  )
}


module.exports.commandShouldFail = function(t, r, cmd, cb) {
  let commandId
  t.test("Run command that should fail: " + cmd.type, t => {
    t.plan(2)
    t.test('Push command', t => {
      t.plan(2)
      cmd.state = "new",
        r.table('newsletter_commands').insert(cmd).run(module.exports.connection).then( result => {
          t.equal(result.inserted, 1, "one command inserted")
          commandId = result.generated_keys[0]
          cb(commandId)
          t.equal(result.generated_keys.length, 1, "one key generated")
        })
    })

    t.test('Wait for command fail', t => {
      t.plan(2)
      r.table('newsletter_commands').get(commandId).changes({ includeInitial: true  }).run(module.exports.connection,
        (err, cursor) => {
          t.equal(err, null, "no error")
          cursor.each((err, result) => {
            if(err) {
              cursor.close();
              t.fail("cursor error: "+ err)
              return false
            }
            let val = result.new_val
            if(val.state == "failed") {
              t.pass("command failed with error: "+ JSON.stringify(val.error))
              cursor.close()
              return false
            }
            if(val.state == "done") {
              t.fail("command not failed, and returned result: "+ JSON.stringify(val.result))
              cursor.close()
              return false
            }
          })
        }
      )
    })
  })

}