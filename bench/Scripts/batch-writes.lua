init = function(args)
	depth = tonumber(args[1]) or 1
	count_of_docs = tonumber(args[2]) or 1
	local reqs = {}
	local doc = "{\"Method\":\"PUT\", \"Type\": \"PUT\", \"Id\":\"temp/\",\"Document\":{\"Name\":\"test1\",\"Supplier\":1,\"Category\":1,\"QuantityPerUnit\":1,\"PricePerUnit\":1,\"UnitsInStock\":1,\"UnitsOnOrder\":1,\"Discontinued\":false,\"ReorderLevel\":1,\"@metadata\":{\"@collection\":\"Temp\"}}}"
	for i=1, depth do
		wrk.path = "/databases/BenchmarkDB/bulk_docs"
        wrk.method = "POST"
        wrk.headers["Content-Type"] = "application/json"
        cmds = "{ \"Commands\": ["
        for i=1, count_of_docs do
        	if i > 1 then
        		cmds = cmds .. ","
        	end 
        	cmds = cmds .. doc
        end
        cmds = cmds .. "]}"
        -- print(cmds)
        wrk.body = cmds
		reqs[i] = wrk.format( nil , method, body, headers)
	end
	req = table.concat(reqs)
end

request = function()
	return req
end 