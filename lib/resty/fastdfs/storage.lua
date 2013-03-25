-- Copyright (C) 2013 Azure Wang
local utils   = require('resty.fastdfs.utils')
local strip_string = utils.strip_string
local fix_string   = utils.fix_string
local buf2int      = utils.buf2int
local int2buf      = utils.int2buf
local read_fdfs_header = utils.read_fdfs_header
local split_fileid = utils.split_fileid
local tcp = ngx.socket.tcp
local string = string
local table  = table
local bit    = bit
local ngx    = ngx
local tonumber = tonumber
local setmetatable = setmetatable
local error = error

module(...)

local VERSION = '0.1'

local FDFS_PROTO_PKG_LEN_SIZE = 8
local FDFS_FILE_EXT_NAME_MAX_LEN = 6
local FDFS_PROTO_CMD_QUIT = 82
local STORAGE_PROTO_CMD_UPLOAD_FILE = 11
local STORAGE_PROTO_CMD_DELETE_FILE = 12
local STORAGE_PROTO_CMD_DOWNLOAD_FILE = 14
local STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE = 21
local STORAGE_PROTO_CMD_QUERY_FILE_INFO = 22
local STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE = 23
local STORAGE_PROTO_CMD_APPEND_FILE = 24

local mt = { __index = _M }

function new(self)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    return setmetatable({ sock = sock }, mt)
end

function connect(self, opts)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    self.group_name = opts.group_name
    self.store_path_index = opts.store_path_index

    local host = opts.host
    local port = opts.port or 23000
    local ok, err = sock:connect(host, port)
    if not ok then
        return nil, err
    end
    return 1   
end
-- upload method
function upload_by_buff(self, buff, ext)
    if not ext then
        return nil, "not ext name"
    end
    ext = fix_string(ext, FDFS_FILE_EXT_NAME_MAX_LEN)
    local size = string.len(buff)
    
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    
    -- send header
    local out = {}
    table.insert(out, int2buf(size + 15))
    table.insert(out, string.char(STORAGE_PROTO_CMD_UPLOAD_FILE))
    -- status
    table.insert(out, "\00")
    -- store_path_index
    table.insert(out, string.char(self.store_path_index))
    -- filesize
    table.insert(out, int2buf(size))
    -- exitname
    table.insert(out, ext)
    -- data
    table.insert(out, buff)
    -- send
    local bytes, err = sock:send(out)
    if not bytes then
        return nil, "tracker send request error:" .. err
    end
    -- read request header
    local hdr, err = read_fdfs_header(sock)
    if not hdr then
        return nil, "read tracker header error:" .. err
    end
    if hdr.len > 0 and hdr.status == 0 then
        local res = {}
        local buf = sock:receive(hdr.len)
        res.group_name = strip_string(string.sub(buf, 1, 16))
        res.file_name  = strip_string(string.sub(buf, 17, hdr.len))
        return res
    else
        return nil, "upload fail:" .. hdr.status
    end
end

function upload_by_sock(self, sock, size, ext)

end

function upload_appender_by_buff(self, buff, ext)
    if not ext then
        return nil, "not ext name"
    end
    ext = fix_string(ext, FDFS_FILE_EXT_NAME_MAX_LEN)
    local size = string.len(buff)

    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    -- send header
    local out = {}
    table.insert(out, int2buf(size + 15))
    table.insert(out, string.char(STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE))
    -- status
    table.insert(out, "\00")
    -- store_path_index
    table.insert(out, string.char(self.store_path_index))
    -- filesize
    table.insert(out, int2buf(size))
    -- exitname
    table.insert(out, ext)
    -- data
    table.insert(out, buff)
    -- send
    local bytes, err = sock:send(out)
    if not bytes then
        return nil, "tracker send request error:" .. err
    end
    -- read request header
    local hdr, err = read_fdfs_header(sock)
    if not hdr then
        return nil, "read tracker header error:" .. err
    end
    if hdr.len > 0 and hdr.status == 0 then
        local res = {}
        local buf = sock:receive(hdr.len)
        res.group_name = strip_string(string.sub(buf, 1, 16))
        res.file_name  = strip_string(string.sub(buf, 17, hdr.len))
        return res
    else
        return nil, "upload fail:" .. hdr.status
    end
end

function upload_appender_by_sock(self, sock, size, ext)

end

function upload_slave_by_buff(self, group_name, file_name, prefix, buff)

end

function upload_slave_by_buff1(self, fileid, prefix, buff)

end

function upload_slave_by_sock(self, group_name, file_name, prefix, sock, size)

end

function upload_slave_by_sock1(self, fileid, prefix, sock, size)

end
-- delete method
function delete_file(self, group_name, file_name)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end 
    if not group_name then
        return nil , "not group_name"
    end
    if not file_name then
        return nil, "not file_name"
    end
    local out = {}
    table.insert(out, int2buf(16 + string.len(file_name)))
    table.insert(out, string.char(STORAGE_PROTO_CMD_DELETE_FILE))
    table.insert(out, "\00")
    -- group name
    table.insert(out, fix_string(group_name, 16))
    -- file name
    table.insert(out, file_name)
    -- send request
    local bytes, err = sock:send(out)
    if not bytes then
        return nil, "storage send request error:" .. err
    end
    -- read request header
    local hdr, err = read_fdfs_header(sock)
    if not hdr then
        return nil, "read tracker header error:" .. err
    end
    if hdr.status == 0 then
        return 1
    else
        return nil
    end
end

function delete_file1(self, fileid)
    local group_name, file_name, err = split_fileid(fileid)
    if not group_name or not file_name then
        return nil, "fileid error:" .. err
    end
    return self:delete_file(group_name, file_name)
end
-- download method
function download_file_to_buff(self, group_name, file_name)

end

function download_file_to_buff1(self, fileid)

end

function download_file_to_sock(self,group_name, file_name, sock)

end

function download_file_to_sock1(self, fileid, sock)

end
-- append method
function append_by_buff(self, group_name, file_name, buff)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    if not group_name then
        return nil , "not group_name"
    end
    if not file_name then
        return nil, "not file_name"
    end
    local file_size = string.len(buff)
    local file_name_len = string.len(file_name)
    -- send request
    local out = {}
    table.insert(out, int2buf(file_size + file_name_len + 16))
    table.insert(out, string.char(STORAGE_PROTO_CMD_APPEND_FILE))
    -- status
    table.insert(out, "\00")
    table.insert(out, int2buf(file_name_len))
    table.insert(out, int2buf(file_size))
    table.insert(out, file_name)
    table.insert(out, buff)
    -- send request
    local bytes, err = sock:send(out)
    if not bytes then
        return nil, "storage send request error:" .. err
    end
    -- read request header
    local hdr, err = read_fdfs_header(sock)
    if not hdr then
        return nil, "read storate header error:" .. err
    end
    if hdr.status == 0 then
        return 1
    else
        return nil, "append fail"
    end
end

function append_by_buff1(self, fileid, buff)
    local group_name, file_name, err = split_fileid(fileid)
    if not group_name or not file_name then
        return nil, "fileid error:" .. err
    end
    return self:append_by_buff(group_name, file_name, buff)
end

function append_by_sock(self, group_name, file_name, sock, size)

end

function append_by_sock1(self, fileid, sock, size)

end

-- modify method
function modify_by_buff(self, group_name, file_name, buff)

end

function modify_by_buff1(self, fileid, buff)

end

function modify_by_sock(self, group_name, file_name, sock, size)

end

function modify_by_sock1(self, fileid, sock, size)

end

-- truncate method
function truncate_file(self, group_name, file_name)

end

function truncate_file1(self, fileid)

end

-- set variavle method
function set_timeout(self, timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    return sock:settimeout(timeout)
end

function set_keepalive(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    return sock:setkeepalive(...)
end

local class_mt = {
    -- to prevent use of casual module global variables
    __newindex = function (table, key, val)
        error('attempt to write to undeclared variable "' .. key .. '"')
    end
}

setmetatable(_M, class_mt)

