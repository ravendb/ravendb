init = function(args)
    depth = tonumber(args[1]) or 1
    wrk.path = "/databases/BenchmarkDB/docs?id=temp/"
        wrk.method = "PUT"
        wrk.body ="{\"Name\":\"test1\",\"Supplier\":1,\"Category\":1,\"QuantityPerUnit\":1,\"PricePerUnit\":1,\"UnitsInStock\":1,\"UnitsOnOrder\":1,\"Discontinued\":false,\"ReorderLevel\":1},\"Metadata\":{\"Raven-Entity-Name\":\"Temp\"}"
        wrk.headers["Content-Type"] = "application/json"
    reqWrite = wrk.format( nil , method, body, headers)
    reqRead = wrk.format(nil, "/databases/BenchmarkDB/docs?&id=temp/000000000000".. math.random(8500000)+1000000 .."-A")
    local result = {}
    local index = 0
    for i=1, depth do
        result[index] = reqRead
        index = index + 1
        result[index] = reqWrite
        index = index + 1
        result[index] = reqRead
        index = index + 1
    end
    req = table.concat(result)
end

request = function()
    return req
end
