-- hash.lua
require("strict")
local newproxy = newproxy
local getmetatable = getmetatable
local next = next

module 'hash'

function new()
  local t={}
  local n=0
  local u=newproxy(true)
  local mt=getmetatable(u)
  mt.__index = function(o,k) return t[k] end
  mt.__newindex = function(o,k,v)
    if t[k]==nil then n=n+1 elseif v==nil then n=n-1 end
    t[k]=v
  end
  mt.__len = function(o) return n end
  mt.__hash = t
  return u
end

function pairs(h)
  return next, getmetatable(h).__hash, nil
end
