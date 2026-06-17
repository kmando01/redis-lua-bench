redis.call('SET', KEYS[1], 'started')
local sum = 0
for i = 1, tonumber(ARGV[1]) do
    sum = sum + i
end
return sum
