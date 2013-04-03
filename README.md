lua-resty-fastdfs
=================

Nonblocking Lua FastDFS driver library for ngx_lua

<pre>

local tracker = require('resty.fastdfs.tracker')
local storage = require('resty.fastdfs.storage')
local tk = tracker:new()
tk:set_timeout(3000)
local ok, err = tk:connect({host='192.168.85.249',port=22122})

function _dump_res(res)
    for i in pairs(res) do
        ngx.say(string.format("%s:%s",i, res[i]))
    end
    ngx.say("")
end

if not ok then
    ngx.say('connect error:' .. err)
    ngx.exit(200)
end

local res, err = tk:query_storage_store()
if not res then
    ngx.say("query storage error:" .. err)
    ngx.exit(200)
end

--[[
local res, err = tk:query_storage_update1("group1/M00/00/00/wKhV-VFbkMQEAAAAAAAAAKbO3LA494.txt")
if not res then
    ngx.say("query storage error:" .. err)
    ngx.exit(200)
end

local res, err = tk:query_storage_fetch1("group1/M00/00/00/wKhV-VFbkFfR1owfAAAAD6bO3LA348.txt")
if not res then
    ngx.say("query storage error:" .. err)
    ngx.exit(200)
end

]]

if not res then
    ngx.say("query storage error:" .. err)
    ngx.exit(200)
end



local st = storage:new()
st:set_timeout(3000)
local ok, err = st:connect(res)
if not ok then
    ngx.say("connect storage error:" .. err)
    ngx.exit(200)
end



local ok, err = st:delete_file1("group1/M00/00/00/wKhV-VFY71sEAAAAAAAAAKbO3LA277.txt")
if not ok then
    ngx.say("Fail:")
else
    ngx.say("OK")
end



local res, err = st:upload_by_buff('abcdedfg','txt')
if not res then
    ngx.say("upload error:" .. err)
    ngx.exit(200)
end


local res, err = st:upload_appender_by_buff('abcdedfg','txt')
if not res then
    ngx.say("upload error:" .. err)
    ngx.exit(200)
end


local ok, err = st:append_by_buff1("group1/M00/00/00/wKhV-VFbkMQEAAAAAAAAAKbO3LA494.txt","abcdedfg\n")
if not ok then
    ngx.say("Fail:")
else
    ngx.say("OK")
end


local res, err = tk:query_storage_update1("group1/M00/00/00/wKhV-VFbkFfR1owfAAAAD6bO3LA348.txt")
if not res then
    ngx.say("query storate error:" .. err)
    ngx.exit(200)
end




</pre>

<h1>
<a name="author" class="anchor" href="#author"><span class="mini-icon mini-icon-link"></span></a>
<a name="author" href="#author"></a>
Author
</h1>

<p>Azure Wang (王非)<a href="mailto:azure1st@gmail.com">azure1st@gmail.com</a></p>

<h1>
<a name="copyright-and-license" class="anchor" href="#copyright-and-license"><span class="mini-icon mini-icon-link"></span></a>
<a name="copyright-and-license" href="#copyright-and-license"></a>
Copyright and License
</h1>

<p>This module is licensed under the BSD license.</p>

<p>Copyright (C) 2013, by Azure Wang (王非) <a href="mailto:azure1st@gmail.com">azure1st@gmail.com</a>.</p>

<p>All rights reserved.</p>

<p>Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:</p>

<ul>
<li>
    <p>Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.</p>
</li>
<li>
    <p>Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.</p>
</li>
</ul><p>THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.</p>
