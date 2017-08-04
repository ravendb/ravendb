init = function(args)
	depth = tonumber(args[1]) or 1	
	local reqs = {}

	for i=1, depth do
		reqs[i] = wrk.format(nil, "/databases/BenchmarkDB/queries?query=From%20Users%20WHERE%20Name='Adi".. math.random(9500000) .."'")
	end

	req = table.concat(reqs)
end

request = function()
	return req
end 
