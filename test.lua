

local function test1(n)

	for i = 1, 1000 do
		local j = i*n
		local f = "test2"..((i % 5) + 1)
		_G[f](j)
	end

end

function test21(n)

	for i = 1, 1000 do
		local j = i*n
	end

end

function test22(n)

	for i = 1, 1000 do
		local j = i*n
	end

end

function test23(n)

	for i = 1, 1000 do
		local j = i*n
	end

end

function test24(n)

	for i = 1, 1000 do
		local j = i*n
	end

end

function test25(n)

	for i = 1, 1000 do
		local j = i*n
	end

end

local function test()

	for i = 1, 300 do
		pcall(test1, i)
	end

end


function start()
    return test()
end

local lperf = require "lperf"
local lstop = lperf.start("lperf.txt", 1)
start()
lstop()