-- redis-cli --eval ./batch-processor.lua source-queue source-job batch-queue batch-job , 1000 10 job-id 1504163093390 2017-08-31T07:04:53.390Z
local sourceQueue = KEYS[1]
local sourceJobName = KEYS[2]
local targetQueue = KEYS[3]
local targetJobName = KEYS[4]
local batchTimeout = tonumber(ARGV[1])
local batchMaxLength = tonumber(ARGV[2])
local batchJobId = ARGV[3]
local currentTime = tonumber(ARGV[4])
local currentTimeISO = ARGV[5]


-- working
local function list_iter(listName, max)

    local all = redis.call("lrange", listName, 0, -1)
    if (max == nil) then max = table.getn(all) end
    local i = 0
    local n = math.min(max, table.getn(all))
    return function()
        i = i + 1
        if (i <= n) then return all[i] end
    end
end

local function hgetall(key)
    local bulk = redis.call('hgetall', key)
    local result = {}
    local nextKey
    for i,v in ipairs(bulk) do
        if i % 2 == 1 then
            nextKey = v
        else
            result[nextKey] = v
        end
    end
    return result
end


local function emitJobEvent(type, jobDataTable)
    local eventTable = {}
    eventTable['type'] = type
    eventTable['timestamp'] = currentTimeISO
    eventTable['job'] = jobDataTable
    redis.call('publish', 'redka-job-events:'..type, cjson.encode(eventTable))
end

local function writeBatchJob(queue, name, params)
    redis.call(
        'hmset', batchJobId,
        'id', batchJobId,
        'queue', queue,
        'name', name,
        'params', params,
        'enqueued', currentTimeISO,
        'delay', 0
    )
    redis.call(
        'lpush', targetQueue..'_pending',
        batchJobId
    )
end

local function flush()
    local nextParams = {}
    -- read list of jobs to move
    for jobid in list_iter(targetQueue..'_batch') do
        -- compile batched job
        redis.call('hmset', jobid, 'dequeued', currentTimeISO, 'complete', currentTimeISO)
        local jobData = hgetall(jobid)
        local jobParams = jobData['params']
        emitJobEvent('DEQUEUED', jobData)
        if jobParams then
            table.insert(nextParams, jobParams)
        end
        emitJobEvent('COMPLETE', jobData)
        redis.call('del', jobid)
    end

    if table.getn(nextParams) ~= 0 then
        writeBatchJob(
            targetQueue..'_pending',
            targetJobName,
            '['..table.concat(nextParams, ',')..']'
        )
    end
    redis.call('del', targetQueue..'_batch')
    -- always refresh timeout
    return redis.call('hset', 'redka__batcher', targetQueue..'_timeout', currentTime + batchTimeout)
end

-- move jobs from pending queue into batch
local function group_into_batch(maxItemsToMove)
    if maxItemsToMove == 0 then return true end
    local sourceList = sourceQueue..'_pending'
    local targetList = targetQueue..'_batch'

    for jobid in list_iter(sourceList, maxItemsToMove) do
        local jobName = redis.call('hget', jobid, 'name')
        if (jobName == sourceJobName) then
           redis.call('lpush', targetList, jobid)
           redis.call('lrem', sourceList, 0, jobid)
        end
    end
    return true
end

local function check_batch_timeout()
    local timeout = redis.call('hget', 'redka__batcher', targetQueue..'_timeout')
    if not timeout then return false end

    if tonumber(timeout) < currentTime then
        return flush()
    end
end

local function check_batch_length()
    local currentLen = redis.call('llen', targetQueue..'_batch')
    if (currentLen >= batchMaxLength) then
        flush()
        return batchMaxLength
    end
    return batchMaxLength - currentLen
end


check_batch_timeout()
local maxItemsToMove = check_batch_length()
group_into_batch(maxItemsToMove)

