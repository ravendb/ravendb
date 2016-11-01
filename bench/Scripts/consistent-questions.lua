init = function(args)
	depth = tonumber(args[1]) or 1	
	local reqs = {}

	for i=1, depth do
		reqs[i] = wrk.format(nil, "/databases/StackOverflow/docs?&id=questions/".. math.random(12350817) .."")
	end

	req = table.concat(reqs)
end

request = function()
	return req
end 