-- Copyright (C) 2013 Azure Wang
local strfind = string.find
local strsub  = string.sub
local strbyte = string.byte
local strrep  = string.rep
local strchar = string.char
local strrep  = string.rep
local band    = bit.band
local bor     = bit.bor
local rshift  = bit.rshift
local lshift  = bit.lshift
local strlen  = string.len

module(...)

local VERSION = '0.1.2'

function  split_fileid(fileid)
    local pos = strfind(fileid, '/')
    if not pos then
        return nil, nil, "fileid not contain /"
    else
        local group_name = strsub(fileid, 1, pos-1)
        local file_name  = strsub(fileid, pos + 1)
        return group_name, file_name
    end
end

function int2buf(n)
    -- only trans 32bit  full is 64bit
    return strrep("\00", 4) .. strchar(band(rshift(n, 24), 0xff), band(rshift(n, 16), 0xff), band(rshift(n, 8), 0xff), band(n, 0xff))
end

function buf2int(buf)
    local c1, c2, c3, c4, c5, c6, c7, c8 = strbyte(buf, 1, 8)
    local lo  = bor(lshift(c5, 24), lshift(c6, 16),lshift(c7, 8), c8)
    local hi = bor(lshift(c1, 24), lshift(c2, 16),lshift(c3, 8), c4)
    return lo + hi * 4294967296
end

function fix_string(str, fix_length)
    if not str then
        return  strrep("\00", fix_length)
    end
    local len = strlen(str)
    if len > fix_length then
        len = fix_length
    end
    local fix_str = strsub(str, 1, len)
    if len < fix_length then
        fix_str = fix_str .. strrep("\00", fix_length - len )
    end
    return fix_str
end

function strip_string(str)
    local pos = strfind(str, "\00")
    if pos then
        return strsub(str, 1, pos - 1)
    else
        return str
    end
end

function read_int(buf, pos)
    return buf2int(strsub(buf, pos, pos + 7)), pos + 8
end

function read_fdfs_header(sock)
    local header = {}
    local buf, err = sock:receive(10)
    if not buf then
        return nil, "read fdfs header error:" .. err
    end
    header.len = buf2int(strsub(buf, 1, 8))
    header.cmd = strbyte(buf, 9)
    header.status = strbyte(buf, 10)
    return header
end

function copy_sock(src, dst, size)
    local copy_count = 0
    local buff_size = 1024 * 32
    while true do
        local chunk, _, part = src:receive(buff_size)
        if not part then
            local bytes, err = dst:send(chunk)
            if not bytes then
                return nil, "copy sock send data error:" .. err
            end
            copy_count = copy_count + bytes
        else
            -- part have data, not read full end
            local bytes, err = dst:send(part)
            if not bytes then
                return nil, "copy sock send data error:" .. err
            end
            copy_count = copy_count + bytes
            break
        end
    end
    if copy_count ~= size then
        -- copy not full
        return nil, "copy sock not full"
    end
    return 1
end
