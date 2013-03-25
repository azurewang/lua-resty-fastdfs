-- Copyright (C) 2013 Azure Wang
local utils   = require('resty.fastdfs.utils')
local tcp = ngx.socket.tcp
local tonumber = tonumber
local setmetatable = setmetatable
local error = error
local strip_string = utils.strip_string
local fix_string   = utils.fix_string
local buf2int      = utils.buf2int
local int2buf      = utils.int2buf
local read_fdfs_header = utils.read_fdfs_header

-- common mix
local string = string
local table  = table
local bit    = bit
local ngx    = ngx

module(...)

local VERSION = '0.1'

local FDFS_PROTO_PKG_LEN_SIZE = 8
local FDFS_FILE_EXT_NAME_MAX_LEN = 6
local FDFS_PROTO_CMD_QUIT = 82
local TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE = 101
local TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE = 102
local TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE = 103
local TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE = 104
local TRACKER_PROTO_CMD_RESP = 100


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
    local host = opts.host
    local port = opts.port or 22122
    local ok, err = sock:connect(host, port)
    if not ok then
        return nil, err
    end
    return 1
end

function query_storage_store(self, group_name)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local out = {}
    if group_name then
        -- query upload with group_name
        -- package length
        table.insert(out, int2buf(16))
        -- cmd
        table.insert(out, string.char(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE))
        -- status
        table.insert(out, "\00")
        -- group name
        table.insert(out, fix_string(group_name, 16))
    else
        -- query upload without group_name
        -- package length
        table.insert(out,  string.rep("\00", FDFS_PROTO_PKG_LEN_SIZE))
        -- cmd
        table.insert(out, string.char(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE))
        -- status
        table.insert(out, "\00")
    end
    -- send request
    local bytes, err = sock:send(out)
    if not bytes then
        return nil, "tracker send request error:" .. err
    end
    -- read request header
    local hdr, err = read_fdfs_header(sock)
    if not hdr then
        return nil, "read tracker header error:" .. err
    end
    -- read data
    if hdr.len > 0 then
        local res = {}
        local buf = sock:receive(hdr.len)
        res.group_name = strip_string(string.sub(buf, 1, 16))
        res.host       = strip_string(string.sub(buf, 17, 31))
        res.port       = buf2int(string.sub(buf, 32, 39))
        res.store_path_index = string.byte(string.sub(buf, 40, 40))
        return res
    else
        return nil, "not receive data"
    end
end

function query_storage_update(self, group_name, file_name)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local out = {}
     -- package length
    table.insert(out, int2buf(16 + string.len(file_name)))
    -- cmd
    table.insert(out, string.char(TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE))
    -- status
    table.insert(out, "\00")
    -- group_name
    table.insert(out, fix_string(group_name, 16))
    -- file name
    table.insert(out, file_name)
    -- send request
    local bytes, err = sock:send(out)
    if not bytes then
        return nil, "tracker send request error:" .. err
    end
    -- read request header
    local hdr, err = read_fdfs_header(sock)
    if not hdr then
        return nil, "read tracker header error:" .. err
    end
    -- read data
    if hdr.len > 0 then
        local res = {}
        local buf = sock:receive(hdr.len)
        res.group_name = strip_string(string.sub(buf, 1, 16))
        res.host       = strip_string(string.sub(buf, 17, 31))
        res.port       = buf2int(string.sub(buf, 32, 39))
        return res
    else
        return nil, "not receive data"
    end
end

function query_storage_fetch(self, group_name, file_name)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local out = {}
    -- package length
    table.insert(out, int2buf(16 + string.len(file_name)))
    -- cmd
    table.insert(out, string.char(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE))
    -- status
    table.insert(out, "\00")
    -- group_name
    table.insert(out, fix_string(group_name, 16))
    -- file name
    table.insert(out, file_name)
    -- send request
    local bytes, err = sock:send(out)
    if not bytes then
        return nil, "tracker send request error:" .. err
    end
    -- read request header
    local hdr, err = read_fdfs_header(sock)
    if not hdr then
        return nil, "read tracker header error:" .. err
    end
    -- read data
    if hdr.len > 0 then
        local res = {}
        local buf = sock:receive(hdr.len)
        res.group_name = strip_string(string.sub(buf, 1, 16))
        res.host       = strip_string(string.sub(buf, 17, 31))
        res.port       = buf2int(string.sub(buf, 32, 39))
        return res
    else
        return nil, "not receive data"
    end
end

function query_storage_update1(self, fileid)
    local group_name, file_name, err = utils.split_fileid(fileid)
    if not group_name or not file_name then
        return nil, "fileid error:" .. err
    end
    return self:query_storage_update(group_name, file_name)
end

function query_storage_fetch1(self, fileid)
    local group_name, file_name, err = utils.split_fileid(fileid)
    if not group_name or not file_name then
        return nil, "fileid error:" .. err
    end
    return self:query_storage_fetch(group_name, file_name)
end

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

