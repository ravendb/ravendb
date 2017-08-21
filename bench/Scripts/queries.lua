init = function(args)
	depth = tonumber(args[1]) or 1	
	local reqs = {}

	for i=1, depth do
		wrk.path = "/databases/BenchmarkDB/queries"
		wrk.method = "POST"
		wrk.body ="{'Query':'FROM Users WHERE Name = :p0','QueryParameters':{'p0':'Adi".. math.random(9500000) .."'}}"
		wrk.headers["Content-Type"] = "application/json"
		reqs[i] = wrk.format(nil , method, body, headers)
	end

	req = table.concat(reqs)
end

request = function()
	return req
end 
