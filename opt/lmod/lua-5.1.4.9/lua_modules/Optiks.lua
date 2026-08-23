-- Optiks.lua

function Optiks_Error(...)
   io.stderr:write("\nError: ")
   for _,v in ipairs{...} do
      io.stderr:write(v)
   end
   io.stderr:write("\n")
   os.exit(1)
end

function Optiks_Exit(v)
   os.exit(v)
end

require("strict")
require("BeautifulTbl")
require("Optiks_Option")

local BeautifulTbl = BeautifulTbl
local Error        = Optiks_Error
local Exit         = Optiks_Exit
local Option       = Optiks_Option
local arg          = arg
local io           = io
local ipairs       = ipairs
local os           = os
local pairs        = pairs
local print        = print
local require      = require
local require      = require
local setmetatable = setmetatable
local systemG      = _G
local table        = table
local tonumber     = tonumber
local tostring     = tostring
local type         = type
local stdout       = io.stdout

module ("Optiks")

local function Prt(...)
   stdout:write(...)
end


icnt = 0

function new(self, t)
   o = {}
   setmetatable(o, self)
   self.__index  = self
   self.argNames = {}
   self.optA     = {}

   local usage   = t
   local version = nil
   if (type(t) == "table") then
      usage   = t.usage
      version = t.version
      Error   = t.error or Error
      Prt     = t.prt or Prt
      Exit    = t.exit or Exit
   end
      

   o.exit    = Exit
   o.prt     = Prt
   o.usage   = usage
   o.version = version
   if (usage == nil) then
      local cmd  = arg[0]
      local i,j  = cmd:find(".*/")
      if (i) then
         cmd = cmd:sub(j+1)
      end
      o.usage = "Usage: " .. cmd .. " [options]"
   end

   o:add_option{
      name    = {"-h", "-?", "--help"},
      dest    = "Optiks_help",
      action  = "store_true",
      help    = "Show this help message and exit",
   }

   if (version) then
      o:add_option{
         name    = {"--version"},
         dest    = "Optiks_version",
         action  = "store_true",
         help    = "Output version info and exit",
      }
   end


   o.dispTbl  = {
      append      = display_store,
      count       = display_count,
      store       = display_store,
      store_true  = display_flag,
      store_false = display_flag,
   }

   return o
end

function add_option(self, myTable)
   local opt   = Option:new(myTable)

   table.insert(self.optA, opt)

   local names = opt:optionNames()

   for i,v in ipairs(names) do
      local _, _, dash, key = v:find("^(%-%-?)([^=-][^=]*)")
      if (self.argNames[key]) then
         Error("duplicate option: \"" .. v .. "\"")
      end
      self.argNames[key] = opt.table
   end
end

function getValue(self, arg, argIn, o, optName)
   local result
   if (arg) then
      result = arg
   else
      result = argIn[1]
      if (result == nil) then
         Error("No value given for option: \"" .. optName .. "\"")
      end
      table.remove(argIn,1)
   end
   if (o.type == "number" ) then
      result = tonumber(result)
   end
   return result
end


function store(self, arg, argIn, argTbl, o, optName)
   return self:getValue(arg,argIn, o, optName)
end

function store_true(self, arg, argIn, argTbl, o, optName)
   return true
end

function append(self, arg, argIn, argTbl, o, optName)
   local result = self:getValue(arg, argIn, o, optName)

   table.insert(argTbl[o.dest], result)

   return argTbl[o.dest]
end

function store_false(self, arg, argIn, argTbl, o, optName)
   return false
end

function count(self, arg, argIn, argTbl, o, optName)
   return argTbl[o.dest] + 1
end

-----------------------------------------------------------
-- Display functions

function display_store(self, opt)
   local a    = {}
   local dest = opt.dest or ""
   local s    = ""
   for _,v in ipairs(opt.name) do
      if (v:len() < 3) then
         s = v .. " " .. dest
      else
         s = v .. "=" .. dest
      end
      a[#a + 1] = s
   end
   return table.concat(a," ")
end

function display_flag(self, opt)
   local a = {}
   for _,v in ipairs(opt.name) do
      a[#a + 1] = v
   end
   return table.concat(a," ")
end

function display_count(self, opt)
   return self:display_flag(opt)
end

function setDefaults(self, argTbl)
   for i,v in ipairs(self.optA) do
      v:setDefault(argTbl)
   end
end

-----------------------------------------------------------
-- The Big Kahuna: This does all the work

function parseOpt(self, optName, arg, argIn, argTbl)
   local o = self.argNames[optName]
   if (o ~= nil) then
      argTbl[o.dest] = self[o.action](self, arg, argIn, argTbl, o, optName)
   else
      Error("Unknown Option: \"" .. optName .. "\"")
   end
end


local function capture(cmd)
   local p = io.popen(cmd)
   if p == nil then
      return nil
   end
   local ret = p:read("*all")
   p:close()
   return ret
end

function printHelp(self)
   local term_width  = capture("tput cols") or "80"
   term_width = tonumber(term_width)
   if (term_width == 0) then
      term_width = 80
   end

   self.prt(self.usage,"\n")
   self.prt("\nOptions:\n")

   local a = {}
   for _,v  in ipairs(self.optA) do
      opt = v.table
      a[#a + 1] = { "  " .. self.dispTbl[opt.action](self, opt), opt.help or " " }
   end
   local bt = BeautifulTbl:new{tbl=a, wrapped=true, column=term_width, prt = self.prt}

   bt:printTbl()
   self.exit(0)
end


function parse(self, argIn)
   local noProcess = nil
   local parg      = {}
   local argTbl    = {[0] = argIn[0]}
   self:setDefaults(argTbl)
   while (argIn[1]) do
      local key = argIn[1]
      table.remove(argIn,1)
      local _, _, dash, optName = key:find("^(%-%-?)([^=-][^=]*)")
      local _, _, arg           = key:find("=(.*)")
      if (key == "--") then
         noProcess = 1
      elseif (dash == nil or noProcess) then
         table.insert(parg, key)
         noProcess = 1
      else
         self:parseOpt(optName, arg, argIn, argTbl)
      end
   end
   if (argTbl.Optiks_help) then
      self:printHelp()
   end
   if (argTbl.Optiks_version) then
      self.prt(self.version .. "\n")
      self.exit()
   end
   return argTbl, parg
end
