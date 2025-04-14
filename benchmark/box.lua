box.cfg{
  listen=3301, 
  memtx_use_mvcc_engine=true
}

if not box.schema.user.exists('test') then
  box.schema.user.create('test')
end

user = box.user
if not user then
  box.schema.user.grant('test', 'execute', 'universe')
end

box.once('grant_user_right', function()
  box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

c = box.space.counter
if not c then
    c = box.schema.space.create('counter', {engine = 'memtx'})
    c:format({
      {name = 'primary', type = 'string'},
      {name = 'num', type = 'unsigned'},
      {name = 'text', type = 'string'}
    })
    pr = c:create_index('primary', {type = 'TREE', unique = true, parts = {1, 'string'}})
    c:insert({'test', 1337, 'Some text.'})
end

c = box.space.counter_vinyl
if not c then
    c = box.schema.space.create('counter_vinyl', {engine = 'vinyl'})
    c:format({
      {name = 'primary', type = 'string'},
      {name = 'num', type = 'unsigned'},
      {name = 'text', type = 'string'}
    })
    pr = c:create_index('primary', {type = 'TREE', unique = true, parts = {1, 'string'}})
    c:insert({'test', 1337, 'Some text.'})
end

s = box.space.bench
if not s then
    s = box.schema.space.create('bench')
    p = s:create_index('primary', {type = 'hash', parts = {1, 'unsigned'}})
end

s = box.space.bench_vinyl
if not s then
    s = box.schema.space.create('bench_vinyl', {engine = 'vinyl'})
    p = s:create_index('primary', {type = 'tree', parts = {1, 'unsigned'}})
end

function clear()
    box.session.su('admin')
    box.space.bench:truncate{}
end