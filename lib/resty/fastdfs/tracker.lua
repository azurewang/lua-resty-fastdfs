-- Copyright (C) 2013 Azure Wang
local utils   = require('resty.fastdfs.utils')
local tcp = ngx.socket.tcp
local setmetatable = setmetatable
local error = error
local strip_string = utils.strip_string
local fix_string   = utils.fix_string
local buf2int      = utils.buf2int
local int2buf      = utils.int2buf
local read_int     = utils.read_int
local read_fdfs_header = utils.read_fdfs_header
local string = string
local table  = table
local date = os.date

module(...)

local VERSION = '0.2.1'

local FDFS_PROTO_PKG_LEN_SIZE = 8
local FDFS_FILE_EXT_NAME_MAX_LEN = 6
local FDFS_PROTO_CMD_QUIT = 82
local TRACKER_PROTO_CMD_SERVER_LIST_ONE_GROUP = 90
local TRACKER_PROTO_CMD_SERVER_LIST_ALL_GROUPS = 91 
local TRACKER_PROTO_CMD_SERVER_LIST_STORAGE = 92
local TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE = 101
local TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE = 102
local TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE = 103
local TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE = 104
local TRACKER_PROTO_CMD_RESP = 100

local FDFS_STORAGE_STATUS_INIT = 0
local FDFS_STORAGE_STATUS_WAIT_SYNC = 1
local FDFS_STORAGE_STATUS_SYNCING = 2
local FDFS_STORAGE_STATUS_IP_CHANGED = 3
local FDFS_STORAGE_STATUS_DELETED = 4
local FDFS_STORAGE_STATUS_OFFLINE = 5
local FDFS_STORAGE_STATUS_ONLINE = 6
local FDFS_STORAGE_STATUS_ACTIVE = 7
local FDFS_STORAGE_STATUS_RECOVERY = 9

local storage_status = {}
storage_status[FDFS_STORAGE_STATUS_INIT]       = "INIT"
storage_status[FDFS_STORAGE_STATUS_WAIT_SYNC]  = "WAIT_SYNC"
storage_status[FDFS_STORAGE_STATUS_SYNCING]    = "SYNCING"
storage_status[FDFS_STORAGE_STATUS_IP_CHANGED] = "IP_CHANGED"
storage_status[FDFS_STORAGE_STATUS_DELETED]    = "DELETED"
storage_status[FDFS_STORAGE_STATUS_OFFLINE]    = "OFFLINE"
storage_status[FDFS_STORAGE_STATUS_ONLINE]     = "ONLINE"
storage_status[FDFS_STORAGE_STATUS_ACTIVE]     = "ACTIVE"
storage_status[FDFS_STORAGE_STATUS_RECOVERY]   = "RECOVERY"

local mt = { __index = _M }

local function format_time(t)
    if t <= 0 then
        return ''
    end
    return date("%Y-%m-%d %H:%M:%S", t)
end

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
        return nil, "error no:" .. hdr.status
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

function list_groups(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local out = {}
    -- body length
    table.insert(out, string.rep("\00", FDFS_PROTO_PKG_LEN_SIZE))
    -- cmd
    table.insert(out, string.char(TRACKER_PROTO_CMD_SERVER_LIST_ALL_GROUPS))
    -- status
    table.insert(out, "\00")
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
    local body_len
    if self.v4 then
        body_len = 105
    else
        body_len = 97
    end
    -- read body
    if hdr.len > 0 then
        if hdr.len % body_len ~= 0 then
            return nil, "response body error"
        end
        local body, err, part = sock:receive(hdr.len)
        if not body then
            return nil, "read response body error"
        end
        local res = {}
        res.count = hdr.len / body_len
        res.groups = {}
        for i=1, res.count do
            local group = {}
            local pos = body_len * (i-1) + 1
            group.group_name = strip_string(string.sub(body, pos, pos + 16))
            pos = pos + 17
            if self.v4 then
                group.total_mb, pos = read_int(body, pos)
            end
            group.free_mb, pos                = read_int(body, pos)
            group.trunk_free_mb, pos          = read_int(body, pos) 
            group.count, pos                  = read_int(body, pos) 
            group.storage_port, pos           = read_int(body, pos)    
            group.storage_http_port, pos      = read_int(body, pos)   
            group.active_count, pos           = read_int(body, pos) 
            group.current_write_server, pos   = read_int(body, pos) 
            group.store_path_count, pos       = read_int(body, pos) 
            group.subdir_count_per_path, pos  = read_int(body, pos) 
            group.current_trunk_file_id, pos  = read_int(body, pos) 
            table.insert(res.groups, group)
        end
        return res
    else
        return nil, "response body is empty"
    end
end

function list_one_group(self, group_name)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local out = {}
    -- body length
    table.insert(out, int2buf(16))
    -- cmd
    table.insert(out, string.char(TRACKER_PROTO_CMD_SERVER_LIST_ONE_GROUP))
    -- status
    table.insert(out, "\00")
    -- group_name
    table.insert(out, fix_string(group_name, 16))
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
    local body_len
    if self.v4 then
        body_len = 105
    else
        body_len = 97
    end
    -- read body
    if hdr.len > 0 then
        if hdr.len ~= body_len then
            return nil, "response body length error"
        end
        local body, err, part = sock:receive(hdr.len)
        if not body then
            return nil, "read response body error"
        end
        local group = {}
        local pos = 1
        group.group_name = strip_string(string.sub(body, pos, pos + 16))
        pos = pos + 17
        if self.v4 then
            group.total_mb, pos = read_int(body, pos)
        end
        group.free_mb, pos                = read_int(body, pos)
        group.trunk_free_mb, pos          = read_int(body, pos) 
        group.count, pos                  = read_int(body, pos) 
        group.storage_port, pos           = read_int(body, pos)    
        group.storage_http_port, pos      = read_int(body, pos)   
        group.active_count, pos           = read_int(body, pos) 
        group.current_write_server, pos   = read_int(body, pos) 
        group.store_path_count, pos       = read_int(body, pos) 
        group.subdir_count_per_path, pos  = read_int(body, pos) 
        group.current_trunk_file_id, pos  = read_int(body, pos) 
        return group
    else
        return nil, "response body is empty"
    end
end

function list_servers(self, group_name)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local out = {}
    -- body length
    table.insert(out, int2buf(16))
    -- cmd
    table.insert(out, string.char(TRACKER_PROTO_CMD_SERVER_LIST_STORAGE))
    -- status
    table.insert(out, "\00")
    -- group_name
    table.insert(out, fix_string(group_name, 16))
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
    if hdr.len > 0 then
        local body_len
        if self.v4 then
            body_len = 600
        else 
            body_len = 584
        end
        if hdr.len  % body_len ~= 0 then
            return nil, "response body length error"
        end
        local body, err, part = sock:receive(hdr.len)
        if not body then
            return nil, "read response body error"
        end
        local res = {}
        res.count = hdr.len / body_len
        res.servers = {}
        for i=1, res.count do
            local server = {}
            local pos = body_len * (i-1) + 1
            server.status = storage_status[string.byte(body, pos)]  or "UNKNOW"
            if self.v4 then
                server.id          = strip_string(string.sub(body, pos+1  , pos+16))
                server.ip_addr     = strip_string(string.sub(body, pos+17 , pos+32))
                server.domain_name = strip_string(string.sub(body, pos+33 , pos+160))
                server.src_id      = strip_string(string.sub(body, pos+161, pos+176))
                pos = pos + 176 + 1
            else
                server.ip_addr     = strip_string(string.sub(body, pos+1  , pos+16))
                server.domain_name = strip_string(string.sub(body, pos+17 , pos+144))
                server.src_ip_addr = strip_string(string.sub(body, pos+145, pos+160))
                pos = pos + 160 + 1
            end
            server.version = strip_string(string.sub(body, pos, pos+5))
            pos = pos + 6
            local tmp
            tmp, pos = read_int(body, pos)
            server.join_time = format_time(tmp)
            tmp, pos = read_int(body, pos)
            server.up_time   = format_time(tmp)
            server.total_mb, pos  = read_int(body, pos)
            server.free_mb, pos                    = read_int(body, pos) 
            server.upload_priority, pos            = read_int(body, pos) 
            server.store_path_count, pos           = read_int(body, pos) 
            server.subdir_count_per_path, pos      = read_int(body, pos) 
            server.current_write_path, pos         = read_int(body, pos) 
            server.storage_port, pos               = read_int(body, pos) 
            server.storage_http_port, pos          = read_int(body, pos) 
            -- FDFSStorageStatBuff
            server.total_upload_count, pos         = read_int(body, pos) 
            server.success_upload_count, pos       = read_int(body, pos) 
            server.total_append_count, pos         = read_int(body, pos) 
            server.success_append_count, pos       = read_int(body, pos) 
            server.total_modify_count, pos         = read_int(body, pos) 
            server.success_modify_count, pos       = read_int(body, pos) 
            server.total_truncate_count, pos       = read_int(body, pos) 
            server.success_truncate_count, pos     = read_int(body, pos) 
            server.total_set_meta_count, pos       = read_int(body, pos) 
            server.success_set_meta_count, pos     = read_int(body, pos) 
            server.total_delete_count, pos         = read_int(body, pos) 
            server.success_delete_count, pos       = read_int(body, pos) 
            server.total_download_count, pos       = read_int(body, pos) 
            server.success_download_count, pos     = read_int(body, pos) 
            server.total_get_meta_count, pos       = read_int(body, pos) 
            server.success_get_meta_count, pos     = read_int(body, pos) 
            server.total_create_link_count, pos    = read_int(body, pos) 
            server.success_create_link_count, pos  = read_int(body, pos) 
            server.total_delete_link_count, pos    = read_int(body, pos) 
            server.success_delete_link_count, pos  = read_int(body, pos) 
            server.total_upload_bytes, pos         = read_int(body, pos) 
            server.success_upload_bytes, pos       = read_int(body, pos) 
            server.total_append_bytes, pos         = read_int(body, pos) 
            server.success_append_bytes, pos       = read_int(body, pos) 
            server.total_modify_bytes, pos         = read_int(body, pos) 
            server.success_modify_bytes, pos       = read_int(body, pos) 
            server.total_download_bytes, pos       = read_int(body, pos) 
            server.success_download_bytes, pos     = read_int(body, pos) 
            server.total_sync_in_bytes, pos        = read_int(body, pos) 
            server.success_sync_in_bytes, pos      = read_int(body, pos) 
            server.total_sync_out_bytes, pos       = read_int(body, pos) 
            server.success_sync_out_bytes, pos     = read_int(body, pos) 
            server.total_file_open_count, pos      = read_int(body, pos) 
            server.success_file_open_count, pos    = read_int(body, pos) 
            server.total_file_read_count, pos      = read_int(body, pos) 
            server.success_file_read_count, pos    = read_int(body, pos) 
            server.total_file_write_count, pos     = read_int(body, pos) 
            server.success_file_write_count, pos   = read_int(body, pos) 
            tmp, pos = read_int(body, pos)
            server.last_source_update = format_time(tmp)
            tmp, pos = read_int(body, pos)
            server.last_sync_update   = format_time(tmp)
            tmp, pos = read_int(body, pos)
            server.last_synced_timestamp = format_time(tmp)
            tmp, pos = read_int(body, pos)
            server.last_heart_beat_time  = format_time(tmp)
            server.if_trunk_server = string.byte(body, pos)
            table.insert(res.servers, server)
        end
        return res
    else
        return nil, "response body is empty"
    end
end

function set_v4(self, flg)
    self.v4 = flg
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

