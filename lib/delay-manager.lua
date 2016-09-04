local function list_iter()
  local all = redis.call("lrange", "redka__global-delay", 0, -1)
  local i = 0
  local n = table.getn(all)
  return function ()
    i = i + 1
    if i <= n then return all[i] end
  end
end

for jobid in list_iter() do
  local delay = redis.call("hget", jobid, "delay")
  if delay <= ARGV[1] then
    local targetqueue = redis.call("hget", jobid, "queue")
    redis.call("lpush", targetqueue .. "_pending", jobid)
    redis.call("lrem", "redka__global-delay", 0, jobid)
  end
end