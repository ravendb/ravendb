init = function(args)
	depth = tonumber(args[1]) or 1
	local reqs = {}
	for i=1, depth do
		wrk.path = "/databases/BenchmarkDB/docs?id=temp/"
        wrk.method = "PUT"
        wrk.body ="{\"Name\":\"test1\",\"Supplier\":1,\"Category\":1,\"QuantityPerUnit\":1,\"PricePerUnit\":1,\"UnitsInStock\":1,\"UnitsOnOrder\":1,\"Discontinued\":false,\"ReorderLevel\":1},\"Metadata\":{\"Raven-Entity-Name\":\"Temp\"}"
        wrk.headers["Content-Type"] = "application/json"
		reqs[i] = wrk.format( nil , method, body, headers)
	end
	req = table.concat(reqs)
end

request = function()
	return req
end 
