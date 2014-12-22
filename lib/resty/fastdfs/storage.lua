-- Copyright (C) 2013 Azure Wang
local utils   = require('resty.fastdfs.utils')
local strip_string = utils.strip_string
local fix_string   = utils.fix_string
local buf2int      = utils.buf2int
local int2buf      = utils.int2buf
local copy_sock    = utils.copy_sock
local read_fdfs_header = utils.read_fdfs_header
local split_fileid = utils.split_fileid
local tcp = ngx.socket.tcp
local string = string
local table  = table
local setmetatable = setmetatable
local error = error

module(...)

local VERSION = '0.2.0'

local FDFS_PROTO_PKG_LEN_SIZE = 8
local FDFS_FILE_EXT_NAME_MAX_LEN = 6
local FDFS_FILE_PREFIX_MAX_LEN = 16
local FDFS_PROTO_CMD_QUIT = 82
local STORAGE_PROTO_CMD_UPLOAD_FILE = 11
local STORAGE_PROTO_CMD_DELETE_FILE = 12
-- local STORAGE_PROTO_CMD_SET_METADATA = 13
local STORAGE_PROTO_CMD_DOWNLOAD_FILE = 14
-- local STORAGE_PROTO_CMD_GET_METADATA = 15
local STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE = 21
local STORAGE_PROTO_CMD_QUERY_FILE_INFO = 22
local STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE = 23
local STORAGE_PROTO_CMD_APPEND_FILE = 24
local STORAGE_PROTO_CMD_MODIFY_FILE = 34
local STORAGE_PROTO_CMD_TRUNCATE_FILE = 36

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
    return true
end

function send_request(self, req, data_sock, size)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local bytes, err = sock:send(req)
    if not bytes then
        return nil, "storage send request error:" .. err
    end
    if data_sock and size then
        local ok, err = copy_sock(data_sock, sock, size)
        if not ok then
            return nil, "storate send data by sock error:" .. err
        end
    end
    return true
end

function read_upload_result(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    -- read request header
    local hdr, err = read_fdfs_header(sock)
    if not hdr then
        return nil, "read storage header error:" .. err
    end
    if hdr.status ~= 0 then
        return nil, "read storage status error:" .. hdr.status
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

function read_update_result(self, op_name)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local hdr, err = read_fdfs_header(sock)
    if not hdr then
        return nil, "read storage header error:" .. err
    end
    if hdr.status == 0 then
        return true
    else
        return nil, op_name .. " error:" .. hdr.status
    end
end

function read_download_result(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local hdr, err = read_fdfs_header(sock)
    if not hdr then
        return nil, "read storage header error:" .. err
    end
    if hdr.status ~= 0 then
        return nil, "read storage status error:" .. hdr.status
    end
    if hdr.len > 0 then
        local data, err, partial = sock:receive(hdr.len)
        if not data then
            return nil, "read file body error:" .. err
        end
        return data
    end
    return ''
end

function read_download_result_cb(self, cb)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    -- read request header
    local hdr, err = read_fdfs_header(sock)
    if not hdr then
        return nil, "read storage header error:" .. err
    end
    if hdr.status ~= 0 then
        return nil, "read storage status error:" .. hdr.status
    end
    local buff_size = 1024 * 16
    local read_size = 0
    local remain = hdr.len
    local out_buf = {}
    while remain > 0 do
        if remain > buff_size then
            read_size = buff_size
            remain = remain - read_size
        else
            read_size = remain
            remain = 0
        end
        local data, err, partial = sock:receive(read_size)
        if not data then
            return nil, "read data error:" .. err
        end
        cb(data)
    end
    return true
end

-- build upload method
local function build_upload_request(cmd, size, ext, path_index)
    local req = {}
    table.insert(req, int2buf(size + 15))  -- length
    table.insert(req, string.char(cmd))    -- command
    table.insert(req, "\00")               -- status
    table.insert(req, string.char(path_index))
    table.insert(req, int2buf(size))
    table.insert(req, fix_string(ext, FDFS_FILE_EXT_NAME_MAX_LEN))
    return req
end

-- upload method
function upload_by_buff(self, buff, ext)
    local size = string.len(buff)
    -- build request
    local req = build_upload_request(STORAGE_PROTO_CMD_UPLOAD_FILE, size, ext, self.store_path_index)
    table.insert(req, buff)
    -- send
    local ok, err = self:send_request(req)
    if not ok then
        return nil, err
    end
    return self:read_upload_result()
end

function upload_by_sock(self, sock, size, ext)
    -- build request
    local req = build_upload_request(STORAGE_PROTO_CMD_UPLOAD_FILE, size, ext, self.store_path_index)
    -- send
    local ok, err = self:send_request(req, sock, size)
    if not ok then
        return nil, err
    end
    return self:read_upload_result()
end

-- uoload_appender method
function upload_appender_by_buff(self, buff, ext)
    local size = string.len(buff)
    -- build request
    local req = build_upload_request(STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, size, ext, self.store_path_index)
    table.insert(req, buff)
    -- send
    local ok, err = self:send_request(req)
    if not ok then
        return nil, err
    end
    return self:read_upload_result()
end

function upload_appender_by_sock(self, sock, size, ext)
    -- build request
    local req = build_upload_request(STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, size, ext, self.store_path_index)
    -- send
    local ok, err = self:send_request(req, sock, size)
    if not ok then
        return nil, err
    end
    return self:read_upload_result()
end

-- build upload_slave_request method
local function build_upload_slave_request(cmd, file_name, prefix, size , ext)
    local req = {}
    table.insert(req, int2buf(16 + FDFS_FILE_PREFIX_MAX_LEN + FDFS_FILE_EXT_NAME_MAX_LEN + string.len(file_name) + size))
    table.insert(req, string.char(cmd))
    table.insert(req, "\00")
    table.insert(req, int2buf(string.len(file_name)))
    table.insert(req, int2buf(size))
    table.insert(req, fix_string(prefix, FDFS_FILE_PREFIX_MAX_LEN))
    table.insert(req, fix_string(ext, FDFS_FILE_EXT_NAME_MAX_LEN))
    table.insert(req, file_name)
    return req
end

-- upload_slave method
function upload_slave_by_buff(self, file_name, prefix, buff, ext)
    local size = string.len(buff)
    if not ext then
        ext = string.match(file_name, "%.(%w+)$")
    end
    -- build request
    local req = build_upload_slave_request(STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, file_name, prefix, size, ext)
    table.insert(req, buff)
    -- send
    local ok, err = self:send_request(req)
    if not ok then
        return nil, err
    end
    return self:read_upload_result()
end

function upload_slave_by_buff1(self, fileid, prefix, buff, ext)
    local group_name, file_name, err = split_fileid(fileid)
    if not group_name or not file_name then
        return nil, "fileid error:" .. err
    end
    return self:upload_slave_by_buff(file_name, prefix, buff, ext)
end

function upload_slave_by_sock(self, file_name, prefix, sock, size, ext)
    if not ext then
        ext = string.match(file_name, "%.(%w+)$")
    end
    local req = build_upload_slave_request(STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, file_name, prefix, size, ext)
    -- send
    local ok, err = self:send_request(req, sock, size)
    if not ok then
        return nil, err
    end
    return self:read_upload_result()
end

function upload_slave_by_sock1(self, fileid, prefix, sock, size, ext)
    local group_name, file_name, err = split_fileid(fileid)
    if not group_name or not file_name then
        return nil, "fileid error:" .. err
    end
    return  self:upload_slave_by_sock(file_name, prefix, sock, size, ext)
end

-- build delete request
local function build_delete_request(cmd, group_name, file_name)
    if not group_name then
        return nil , "not group_name"
    end 
    if not file_name then
        return nil, "not file_name"
    end
    local req = {}
    table.insert(req, int2buf(16 + string.len(file_name)))
    table.insert(req, string.char(cmd))
    table.insert(req, "\00")
    table.insert(req, fix_string(group_name, 16))
    table.insert(req, file_name)
    return req
end

-- delete method
function delete_file(self, group_name, file_name)
    local req, err = build_delete_request(STORAGE_PROTO_CMD_DELETE_FILE, group_name, file_name)
    if not req then
        return nil, err
    end
    -- send request
    local ok, err = self:send_request(req)
    if not ok then
        return nil, err
    end
    return self:read_update_result("delete_file")
end

function delete_file1(self, fileid)
    local group_name, file_name, err = split_fileid(fileid)
    if not group_name or not file_name then
        return nil, "fileid error:" .. err
    end
    return self:delete_file(group_name, file_name)
end

-- build truncate method
local function build_truncate_request(cmd, file_name, remain_bytes)
    if not file_name then
        return nil, "not file_name"
    end
    local file_name_len = string.len(file_name)
    local req = {}
    table.insert(req, int2buf(16 + file_name_len))
    table.insert(req, string.char(cmd))
    table.insert(req, "\00")
    table.insert(req, int2buf(file_name_len)) 
    table.insert(req, int2buf(remain_bytes))
    table.insert(req, file_name)
    return req
end
-- truncate method
function truncate_file(self, file_name)
    local req, err = build_truncate_request(STORAGE_PROTO_CMD_TRUNCATE_FILE, file_name, 0)
    if not req then
        return nil, err
    end
    -- send request
    local ok, err = self:send_request(req)
    if not ok then
        return nil, err
    end
    return self:read_update_result("truncate_file")  
end

function truncate_file1(self, fileid)
    local group_name, file_name, err = split_fileid(fileid)
    if not group_name or not file_name then
        return nil, "fileid error:" .. err
    end
    return self:truncate_file(file_name)
end

-- build download request
local function build_download_request(cmd, group_name, file_name)
    if not group_name then
        return nil , "not group_name"
    end
    if not file_name then
        return nil, "not file_name"
    end
    local req = {}
    table.insert(req, int2buf(32 + string.len(file_name)))
    table.insert(req, string.char(cmd))
    table.insert(req, "\00")
    table.insert(req, string.rep("\00", 16))    -- download file offset
    table.insert(req, fix_string(group_name, 16))
    table.insert(req, file_name)
    return req
end

-- download method
function download_file_to_buff(self, group_name, file_name)
    local req, err = build_download_request(STORAGE_PROTO_CMD_DOWNLOAD_FILE, group_name, file_name)
    if not req then
        return nil, err
    end
    -- send
    local ok, err = self:send_request(req)
    if not ok then
        return nil, err
    end
    return self:read_download_result()
end

function download_file_to_buff1(self, fileid)
    local group_name, file_name, err = split_fileid(fileid)
    if not group_name or not file_name then
        return nil, "fileid error:" .. err
    end
    return self:download_file_to_buff(group_name, file_name)
end

function download_file_to_callback(self,group_name, file_name, cb)
    local req, err = build_download_request(STORAGE_PROTO_CMD_DOWNLOAD_FILE, group_name, file_name)
    if not req then
        return nil, err
    end
    -- send
    local ok, err = self:send_request(req)
    if not ok then
        return nil, err
    end
    return self:read_download_result_cb(cb)
end

function download_file_to_callback1(self, fileid, cb)
    local group_name, file_name, err = split_fileid(fileid)
    if not group_name or not file_name then
        return nil, "fileid error:" .. err
    end
    return self:download_file_to_callback(group_name, file_name, cb)
end

-- build append request
local function build_append_request(cmd, group_name, file_name, size)
    if not group_name then
        return nil , "not group_name"
    end
    if not file_name then
        return nil, "not file_name"
    end
    local file_name_len = string.len(file_name)
    local req = {}
    table.insert(req, int2buf(16 + size + file_name_len))
    table.insert(req, string.char(cmd))
    table.insert(req, "\00")
    table.insert(req, int2buf(file_name_len))
    table.insert(req, int2buf(size))
    table.insert(req, file_name)
    return req
end

-- append method
function append_by_buff(self, group_name, file_name, buff)
    local size = string.len(buff)
    local req, err = build_append_request(STORAGE_PROTO_CMD_APPEND_FILE, group_name, file_name, size)
    if not req then
        return nil, err
    end
    table.insert(req, buff)
    -- send request
    local ok, err = self:send_request(req)
    if not ok then
        return nil, err
    end
    return self:read_update_result("append_by_buff")
end

function append_by_buff1(self, fileid, buff)
    local group_name, file_name, err = split_fileid(fileid)
    if not group_name or not file_name then
        return nil, "fileid error:" .. err
    end
    return self:append_by_buff(group_name, file_name, buff)
end

function append_by_sock(self, group_name, file_name, sock, size)
    local req, err = build_append_request(STORAGE_PROTO_CMD_APPEND_FILE, group_name, file_name, size)
    if not req then
        return nil, err
    end
    -- send data
    local ok, err = self:send_request(req, sock, size)
    if not ok then
        return nil, err
    end
    return self:read_update_result("append_by_sock")
end

function append_by_sock1(self, fileid, sock, size)
    local group_name, file_name, err = split_fileid(fileid)
    if not group_name or not file_name then
        return nil, "fileid error:" .. err
    end
    return self:append_by_sock(group_name, file_name, sock, size)
end

-- build modify request
local function build_modify_request(cmd, file_name, offset, size)
    if not file_name then
        return nil, "not file_name"
    end
    local file_name_len = string.len(file_name)
    local req = {}
    table.insert(req, int2buf(size + file_name_len + 24))
    table.insert(req, string.char(STORAGE_PROTO_CMD_MODIFY_FILE))
    table.insert(req, "\00")
    table.insert(req, int2buf(file_name_len))
    table.insert(req, int2buf(offset))
    table.insert(req, int2buf(size))
    table.insert(req, file_name)
    return req
end

-- modify method
function modify_by_buff(self, file_name, buff, offset)
    local size = string.len(buff)
    local req, err = build_modify_request(STORAGE_PROTO_CMD_MODIFY_FILE, file_name, offset, size)
    table.insert(req, buff)
    if not req then
        return nil, err
    end
    -- send request
    local ok, err = self:send_request(req)
    if not ok then
        return nil, err
    end
    return self:read_update_result("modify_by_buff")
end

function modify_by_buff1(self, fileid, buff, offset)
    local group_name, file_name, err = split_fileid(fileid)
    if not group_name or not file_name then
        return nil, "fileid error:" .. err
    end
    return self:modify_by_buff(file_name, buff, offset)
end

function modify_by_sock(self, file_name, sock, size, offset)
    local req, err = build_modify_request(STORAGE_PROTO_CMD_MODIFY_FILE, file_name, offset, size)
    if not req then
        return nil, err
    end
    -- send data
    local ok, err = self:send_request(req, sock, size)
    if not ok then
        return nil, err
    end
    return self:read_update_result("modify_by_sock")
end

function modify_by_sock1(self, fileid, sock, size, offset)
    local group_name, file_name, err = split_fileid(fileid)
    if not group_name or not file_name then
        return nil, "fileid error:" .. err
    end
    return self:modify_by_sock(file_name, sock, size, offset)
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
