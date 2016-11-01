init = function(args)
	depth = tonumber(args[1]) or 1	
	math.randomseed(os.time())
end

request = function()
	local reqs = {}

	for i=1, depth do
		if math.random(2) == 1 then
			reqs[i] = wrk.format(nil, "/databases/StackOverflow/docs?&id=questions/".. math.random(12350817) .."")
		else
	   	  	reqs[i] = wrk.format(nil, "/databases/StackOverflow/docs?&id=users/".. math.random(5987285) .."")
	   	end
	end
	return table.concat(reqs)
end 