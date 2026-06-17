#!lua name=counterlib
redis.register_function('limit_incr', function(keys, args)
    local current = tonumber(redis.call('GET', keys[1]) or '0')
    if current >= tonumber(args[1]) then return -1 end
    return redis.call('INCR', keys[1])
end)
redis.register_function('get_val', function(keys, args)
    return redis.call('GET', keys[1])
end)
