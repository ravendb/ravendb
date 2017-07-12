-- https://github.com/czerasz/wrk-debugging-environment

-- Helper Functions:

-- Resource: http://lua-users.org/wiki/TypeOf
function typeof(var)
    local _type = type(var);
    if(_type ~= "table" and _type ~= "userdata") then
        return _type;
    end
    local _meta = getmetatable(var);
    if(_meta ~= nil and _meta._NAME ~= nil) then
        return _meta._NAME;
    else
        return _type;
    end
end

-- Resource: https://gist.github.com/lunixbochs/5b0bb27861a396ab7a86
local function string(o)
    return '"' .. tostring(o) .. '"'
end
 
local function recurse(o, indent)
    if indent == nil then indent = '' end
    local indent2 = indent .. '  '
    if type(o) == 'table' then
        local s = indent .. '{' .. '\n'
        local first = true
        for k,v in pairs(o) do
            if first == false then s = s .. ', \n' end
            if type(k) ~= 'number' then k = string(k) end
            s = s .. indent2 .. '[' .. k .. '] = ' .. recurse(v, indent2)
            first = false
        end
        return s .. '\n' .. indent .. '}'
    else
        return string(o)
    end
end
 
local function var_dump(...)
    local args = {...}
    if #args > 1 then
        var_dump(args)
    else
        print(recurse(args[1]))
    end
end

-- @end: Helper Functions

max_requests = 0
counter = 1

function setup(thread)
   thread:set("id", counter)
   
   counter = counter + 1
end

init = function(args)
  io.write("[init]\n")

  -- Check if arguments are set
  if not (next(args) == nil) then
    io.write("[init] Arguments\n")

    -- Loop through passed arguments
    for index, value in ipairs(args) do
      io.write("[init]  - " .. args[index] .. "\n")
    end
  end
end

response = function (status, headers, body)
  io.write("------------------------------\n")
  io.write("Response ".. counter .." with status: ".. status .." on thread ".. id .."\n")
  io.write("------------------------------\n")

  io.write("[response] Headers:\n")

  -- Loop through passed arguments
  for key, value in pairs(headers) do
    io.write("[response]  - " .. key  .. ": " .. value .. "\n")
  end

  io.write("[response] Body:\n")
  io.write(body .. "\n")

  -- Stop after max_requests if max_requests is a positive number
  if (max_requests > 0) and (counter > max_requests) then
    wrk.thread:stop()
  end

  counter = counter + 1
end

done = function (summary, latency, requests)
  io.write("------------------------------\n")
  io.write("Requests\n")
  io.write("------------------------------\n")

  io.write(typeof(requests))

  var_dump(summary)
  var_dump(requests)
end
