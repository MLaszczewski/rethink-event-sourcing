
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

module.exports.runCommand = function(t, r, table, cmd, cb) {
  return new Promise( (resolve, reject) => {
    let commandId
    t.test("Run command " + cmd.type, t => {
      t.plan(2)
      t.test('Push command', t => {
        t.plan(2)
        cmd.state = "new"
        cmd.timestamp = new Date()
        r.table(table + '_commands').insert(cmd).run(module.exports.connection).then( result => {
          t.equal(result.inserted, 1, "one command inserted")
          commandId = result.generated_keys[0]
          if(cb) cb(commandId)
          t.equal(result.generated_keys.length, 1, "one key generated")
        })
      })

      t.test('Wait for command done', t => {
        t.plan(2)
        r.table(table + '_commands').get(commandId).changes({ includeInitial: true  }).run(module.exports.connection,
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
                resolve(val.result)
                return false
              }
              if(val.state == "failed") {
                t.fail("command failed with error: "+ JSON.stringify(val.error))
                cursor.close()
                reject(val.error)
                return false
              }
            })
          }
        )
      })
    })
    
  })

}

module.exports.getGeneratedEvents = function(r, table, commandId, cb) {
  r.table(table + '_events').filter(r.row('commandId').eq(commandId)).run(module.exports.connection).then(
    results => results.toArray().then(
      eventBuckets => {
        cb(Array.prototype.concat.apply([], eventBuckets.map(bucket => {
          if(bucket.events) return bucket.events
          return [bucket]
        })))
      }
    )
  )
}

module.exports.commandShouldFail = function(t, r, table, cmd, cb) {
  let commandId
  t.test("Run command that should fail: " + cmd.type, t => {
    t.plan(2)
    t.test('Push command', t => {
      t.plan(2)
      cmd.state = "new",
        r.table(table + '_commands').insert(cmd).run(module.exports.connection).then( result => {
          t.equal(result.inserted, 1, "one command inserted")
          commandId = result.generated_keys[0]
          cb(commandId)
          t.equal(result.generated_keys.length, 1, "one key generated")
        })
    })

    t.test('Wait for command fail', t => {
      t.plan(2)
      r.table(table + '_commands').get(commandId).changes({ includeInitial: true  }).run(module.exports.connection,
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

module.exports.pushEvents = function(t, r, table, events, commandId) {
  t.test("push events", t=> {
    t.plan(2)

    let lastId
    t.test('pull last inserted event id', t => {
      t.plan(1)
      r.table(table + '_events').orderBy(r.desc(r.row('id'))).limit(1)('id').run(module.exports.connection).then(
        ([ id ]) => {
          if(id === undefined) id = 0
          lastId = id
          t.pass("last inserted id: " + id)
        }
      )
    })
    if(events.length == 1) {
      let event = events[0]
      event.commandId = commandId || null
      t.test('push single event of type ' + event.type + ' to service ' + table, t => {
        t.plan(1)
        event.id = ++lastId
        r.table(table + '_events').insert(event).run(module.exports.connection).then( result => {
          t.equal(result.inserted, 1, "one event inserted")
        })
      })
    } else {
      t.test('push events bucket with types ' + events.map(e=>e.type).join(", ") + ' to service ' + table, t => {
        t.plan(1)
        let bucket = {}
        for(let event of events) {
          for(let k in event) bucket[k] = event[k]
        }
        bucket.type = 'bucket'
        bucket.timestamp = new Date
        bucket.commandId = commandId || null
        bucket.events = events
        bucket.id = ++lastId

        r.table(table + '_events').insert(bucket).run(module.exports.connection).then( result => {
          t.equal(result.inserted, 1, "events bucket inserted")
        })
      })
    }
    
  })
}
