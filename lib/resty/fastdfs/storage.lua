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

local VERSION = '0.1.1'

local FDFS_PROTO_PKG_LEN_SIZE = 8
local FDFS_FILE_EXT_NAME_MAX_LEN = 6
local FDFS_FILE_PREFIX_MAX_LEN = 16
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
    return 1
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
        return 1
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
    return 1
end

-- upload method
function upload_by_buff(self, buff, ext)
    local size = string.len(buff)
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
    table.insert(out, fix_string(ext, FDFS_FILE_EXT_NAME_MAX_LEN))
    -- data
    table.insert(out, buff)
    -- send
    local ok, err = self:send_request(out)
    if not ok then
        return nil, err
    end
    return self:read_upload_result()
end

function upload_by_sock(self, sock, size, ext)
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
    table.insert(out, fix_string(ext, FDFS_FILE_EXT_NAME_MAX_LEN))
    -- send
    local ok, err = self:send_request(out, sock, size)
    if not ok then
        return nil, err
    end
    return self:read_upload_result()
end

function upload_appender_by_buff(self, buff, ext)
    local size = string.len(buff)
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
    table.insert(out, fix_string(ext, FDFS_FILE_EXT_NAME_MAX_LEN))
    -- data
    table.insert(out, buff)
    -- send
    local ok, err = self:send_request(out)
    if not ok then
        return nil, err
    end
    return self:read_upload_result()
end

function upload_appender_by_sock(self, sock, size, ext)
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
    table.insert(out, fix_string(ext, FDFS_FILE_EXT_NAME_MAX_LEN))
    -- send
    local ok, err = self:send_request(out, sock, size)
    if not ok then
        return nil, err
    end
    return self:read_upload_result()
end

function upload_slave_by_buff(self, group_name, file_name, prefix, buff, ext)
    if not group_name then
        return nil , "not group_name"
    end
    if not file_name then
        return nil, "not file_name"
    end
    -- default ext is the same as file_name
    if not ext then
        ext = string.match(file_name, "%.(%w+)$")
    end
    -- master_filename_len file_size prefix ext_name master_filename body
    local out = {}
    table.insert(out, int2buf(16 + FDFS_FILE_PREFIX_MAX_LEN + FDFS_FILE_EXT_NAME_MAX_LEN + string.len(file_name) + string.len(buff)))
    table.insert(out, string.char(STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE))
    table.insert(out, "\00")
    table.insert(out, int2buf(string.len(file_name)))
    table.insert(out, int2buf(string.len(buff)))
    table.insert(out, fix_string(prefix, FDFS_FILE_PREFIX_MAX_LEN))
    table.insert(out, fix_string(ext, FDFS_FILE_EXT_NAME_MAX_LEN))
    table.insert(out, file_name)
    table.insert(out, buff)
    -- send
    local ok, err = self:send_request(out)
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
    return self:upload_slave_by_buff(group_name, file_name, prefix, buff, ext)
end

function upload_slave_by_sock(self, group_name, file_name, prefix, sock, size, ext)
    if not group_name then
        return nil , "not group_name"
    end
    if not file_name then
        return nil, "not file_name"
    end
    -- default ext is the same as file_name
    if not ext then
        ext = string.match(file_name, "%.(%w+)$")
    end
    -- master_filename_len file_size prefix ext_name master_filename body
    local out = {}
    table.insert(out, int2buf(16 + FDFS_FILE_PREFIX_MAX_LEN + FDFS_FILE_EXT_NAME_MAX_LEN + string.len(file_name) + size))
    table.insert(out, string.char(STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE))
    table.insert(out, "\00")
    table.insert(out, int2buf(string.len(file_name)))
    table.insert(out, int2buf(size))
    table.insert(out, fix_string(prefix, FDFS_FILE_PREFIX_MAX_LEN))
    table.insert(out, fix_string(ext, FDFS_FILE_EXT_NAME_MAX_LEN))
    table.insert(out, file_name)
    -- send
    local ok, err = self:send_request(out, sock, size)
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
    return  self:upload_slave_by_sock(group_name, file_name, prefix, sock, size, ext)
end
-- delete method
function delete_file(self, group_name, file_name)
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
    local ok, err = self:send_request(out)
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
-- download method
function download_file_to_buff(self, group_name, file_name)
    if not group_name then
        return nil , "not group_name"
    end
    if not file_name then
        return nil, "not file_name"
    end
    local out = {}
    -- file_offset(8)  download_bytes(8)  group_name(16)  file_name(n)
    table.insert(out, int2buf(32 + string.len(file_name)))
    table.insert(out, string.char(STORAGE_PROTO_CMD_DOWNLOAD_FILE))
    table.insert(out, "\00")
    -- file_offset  download_bytes  8 + 8
    table.insert(out, string.rep("\00", 16))
    -- group name
    table.insert(out, fix_string(group_name, 16))
    -- file name
    table.insert(out, file_name)
    -- send
    local ok, err = self:send_request(out)
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
    if not group_name then
        return nil , "not group_name"
    end
    if not file_name then
        return nil, "not file_name"
    end
    local out = {}
    -- file_offset(8)  download_bytes(8)  group_name(16)  file_name(n)
    table.insert(out, int2buf(32 + string.len(file_name)))
    table.insert(out, string.char(STORAGE_PROTO_CMD_DOWNLOAD_FILE))
    table.insert(out, "\00")
    -- file_offset  download_bytes  8 + 8
    table.insert(out, string.rep("\00", 16))
    -- group name
    table.insert(out, fix_string(group_name, 16))
    -- file name
    table.insert(out, file_name)
    -- send
    local ok, err = self:send_request(out)
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

-- append method
function append_by_buff(self, group_name, file_name, buff)
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
    local ok, err = self:send_request(out, sock, size)
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
    -- send data
    local ok, err = self:send_request(out, sock, size)
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
    return self:append_by_buff(group_name, file_name, sock, size)
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

