-- $Id$ --

-------------------------------------------------------------------------
--   Test code for using  Dbg.lua
-------------------------------------------------------------------------

--    require("strict")
--    require("Dbg")
--    function a()
--       local dbg   = Dbg:dbg()
--       dbg.start(2,"a")
--       dbg.print("In a","\n")
--       b()
--       dbg.fini()
--    end
--    
--    function b()
--       local dbg   = Dbg:dbg()
--       dbg.start(2,"b")
--       dbg.print("In b","\n")
--       c()
--       dbg.fini()
--    end
--    
--    function c()
--       local dbg   = Dbg:dbg()
--       dbg.start(3,"c")
--       dbg.print(1,"In c","\n")
--       dbg.fini()
--    end
--    
--    function main()
--       local level = 10
--       local dbg   = Dbg:dbg()
--       dbg:activateDebug(level)
--    
--       dbg.start(2,"main")
--       a()
--       dbg.fini()
--    end
--    
--    main()

local blank        = " "
local huge         = math.huge
local io           = io
local ipairs       = ipairs
local max          = math.max
local os           = os
local select       = select
local setmetatable = setmetatable
local tostring     = tostring
local type         = type
local remove       = table.remove

Dbg = {}
require("strict")

module("Dbg")

s_dbg = nil
s_indentString = ""
s_indentLevel  = 0
s_vpl          = 1
s_currentLevel = huge
s_levelA       = {}
local function prtTbl(a)
   io.stderr:write("table:\n")
   for _,v in ipairs(a) do
      if (type(a) == "table") then
	 prtTbl(v)
      else
	 io.stderr:write(v)
      end
   end
end

local function new(self)
   local o = {}
   setmetatable(o,self)
   self.__index   = self
   o.print        = Quiet
   o.start        = Quiet
   o.fini         = Quiet
   o.warning      = Warning
   o.error        = Error
   o.quiet        = Quiet
   if (t and type(t) == 'table') then
      o.vpl       = t.vpl
   end
   return o
end

function dbg(self)
   if (s_dbg == nil) then
      s_dbg = new(self)
   end
   return s_dbg
end

function activateDebug(self, level)
   level = level or 1
   if (level > 0) then
      self.print            = Debug
      self.start            = Start
      self.fini             = Fini
      s_currentLevel        = level
      s_levelA[#s_levelA+1] = level
   end
end

function currentLevel(level)
   s_currentLevel = level or 1
end

function deactivateWarning(self)
   self.warning = Quiet
end

function activateWarning(self)
   self.warning = Warning
end

function Quiet()
end

local function extractVPL(...)
   local vpl = s_vpl
   local firstV = select(1,...)
   if (type(firstV) == "number") then
      vpl = firstV
   end
   return vpl
end   

local function startExtractVPL(...)
   local vpl = s_vpl
   local firstV = select(1,...)
   if (type(firstV) == "number") then
      vpl = firstV
   end
   s_levelA[#s_levelA+1] = vpl
   return vpl
end   

function Start(...)
   s_vpl = startExtractVPL(...)
   if (s_vpl <= s_currentLevel) then 
      io.stderr:write(s_indentString)
      for i,v in ipairs{...} do
         if (i ~= 1 or type(v) ~= "number") then
            io.stderr:write(v)
         end
      end
      io.stderr:write("{\n")
      s_indentLevel  = s_indentLevel + 1
      s_indentString = blank:rep(s_indentLevel*2)
   end
end

function Fini()
   local vpl = s_vpl
   if (#s_levelA > 1) then
      remove(s_levelA)  -- remove last entry in table
   end
   s_vpl = s_levelA[#s_levelA]

   if (vpl <= s_currentLevel) then 
      s_indentLevel  = max(0, s_indentLevel - 1)
      s_indentString = blank:rep(s_indentLevel*2)
      io.stderr:write(s_indentString,"}\n")
   end
end

function Warning(...)
   io.stderr:write("\nWarning: ")
   for _,v in ipairs{...} do
      io.stderr:write(v)
   end
end

function Error(...)
   io.stderr:write("\nError: ")
   for _,v in ipairs{...} do
      io.stderr:write(v)
   end
   io.stderr:write("\n")
   errorExit()
end

function errorExit()
   io.stdout:write("false\n")
   os.exit(1)
end
   


function Debug(...)
   vpl = extractVPL(...)
   if (vpl > s_currentLevel) then 
      return 
   end

   io.stderr:write(s_indentString)
   for i,v in ipairs{...} do
      if (type(v) == "table") then
	 prtTbl(v)
      elseif (i == 1 and type(v) == 'number') then
         -- do nothing
      else
         if (type(v) ~= "string") then
            v = tostring(v)
         end
         local idx = v:find("\n")
         if (idx == nil or v:len() == idx) then
            io.stderr:write(v)
         else
            local s = v:sub(1,idx)
            io.stderr:write(s)
            Debug(v:sub(idx+1))
         end
      end
   end
end

function flush()
   io.stderr:flush()
end
