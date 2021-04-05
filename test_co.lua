
function test_loop(n)
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


function test1()
    for i = 1, 300 do
		pcall(test_loop, i)
	end
end

function test2()
    for i = 1, 300 do
		pcall(test_loop, i)
	end
end

function start()
    test1()
    co = coroutine.create(test2)
    print(coroutine.resume(co))
end

local lperf = require "lperf"
local lstop = lperf.start("lperf_co.txt", 1)
start()
lstop()