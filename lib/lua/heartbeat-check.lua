local cutoff = ARGV[1] - (10 * 1000)

-- resolve keys and filter out *_progress lists
local function progress_lists_iter()
    -- ok to use keys here as script itself is blocking
    local lists = redis.call('keys', '*_progress')
    local i = 0
    local n = table.getn(lists)
    return function()
        i = i + 1
        if i <= n then return lists[i] end
    end
end

-- iterate over ids in all progress lists and compare heartbeat value to argv[1]
local function list_iterms_iter(list_key)
    local all = redis.call('lrange', list_key, 0, -1)
    local i = 0
    local n = table.getn(all)
    return function ()
        i = i + 1
        if i <= n then return all[i] end
    end
end

local function check_heartbeat(jobid)
    local lastHeartbeat = tonumber(redis.call('hget', jobid, 'heartbeat'))
    if (lastHeartbeat == nil or lastHeartbeat == '') then return false end

    if lastHeartbeat <= cutoff then
        redis.call('hset', jobid, 'wasOnceStuck', cutoff)
        local targetqueue = redis.call('hget', jobid, 'queue')
        if (type(targetqueue) == 'string') then
            redis.call('lpush', targetqueue .. '_pending', jobid)
            redis.call('lrem', targetqueue .. '_progress', 0, jobid)
            local currentAttempt = redis.call('hget', jobid, 'attempt')
            if currentAttempt == nil or currentAttempt == '' then
                currentAttempt = 1
            end
            local nextAttempt = currentAttempt + 1
            redis.call('hset', jobid, 'attempt', nextAttempt)
            return true
        end
    end

    return false
end

local requeued = false
for progress_list in progress_lists_iter() do
    for jobid in list_iterms_iter(progress_list) do
        local result = check_heartbeat(jobid)
        if (result == true) then requeued = true end
    end
end

return requeued