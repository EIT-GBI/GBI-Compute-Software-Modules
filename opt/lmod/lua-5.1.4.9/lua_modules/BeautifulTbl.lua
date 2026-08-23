-- Beautiful_tbl.lua

require("strict")
require("string_split")
local setmetatable = setmetatable
local concat	   = table.concat
local ipairs	   = ipairs
local pairs	   = pairs
local type	   = type
local print	   = print
local max	   = math.max
local string	   = string
local stdout       = io.stdout
BeautifulTbl = { gap = 2}

local blank = ' '

module ('BeautifulTbl')

function prt(...)
   stdout:write(...)
end



function new(self, t)
   local tbl = t
   local o = {}
   if (t.tbl) then
      tbl = t.tbl
      o   = t
   end

   setmetatable(o, self)
   self.__index  = self

   prt        = t.prt or prt
   o.tbl      = o:build_tbl(tbl)
   o.printTbl = print_tbl
   o.column   = o.column  or 0
   o.wrapped  = o.wrapped or false
   return o
end

function print_tbl(self)

   local width = 0
   local simple = true
   if (self.wrapped and self.column > 0) then
      for i = 1, #self.columnCnt-1 do
         width = width + self.columnCnt[i]
      end
      local last = self.columnCnt[#self.columnCnt]
      simple = (width > self.column-20) or (width + last < self.column)
   end

   if (simple) then
      for _, a in ipairs(self.tbl) do
         for _, v in ipairs(a) do
            prt(v)
         end
         prt("\n")
      end
      return
   end

   self.column = self.column - 1
   local gap = self.column - width
   local fill=string.rep(" ",width+self.gap*(#self.columnCnt-1))

   -- printing a wrapped last column
   for _, a in ipairs(self.tbl) do
      local aa = {}
      for i = 1, #a-1 do
         aa[#aa+1] = a[i]
      end

      local icnt = width
      local s = a[#a]
      for w in s:split("%s+") do
         local wlen = w:len()+1
         if (icnt + wlen < self.column or wlen > gap) then
            aa[#aa+1] = w .. " "
         else
            aa[#aa+1] ="\n"
            prt(concat(aa,""))
            aa    = {}
            aa[1] = fill
            icnt  = width
            aa[2] = w .. " "
         end
         icnt = icnt + wlen
      end
      aa[#aa+1] ="\n"
      prt(concat(aa,""))
   end
end

function build_tbl(self,tblIn)

   local columnCnt = {}
   local maxnc     = 1
   local tbl       = {}
   local icol,irow

   for _,a  in ipairs(tblIn) do
      icol = 0
      for _, v in ipairs(a) do
	 icol = icol + 1
	 columnCnt[icol] = max(v:len(), columnCnt[icol] or 0)
      end
   end

   maxnc = #columnCnt
   for _,a  in ipairs(tblIn) do
      icol = 0
      irow = #tbl+1
      tbl[irow] = {}

      for _,v in ipairs(a) do
	 icol = icol + 1
         if (icol < maxnc) then
            local nspaces = columnCnt[icol] - v:len() + self.gap
            tbl[irow][icol] = v .. blank:rep(nspaces)
         else
            tbl[irow][icol] = v
         end
      end
   end

   self.columnCnt = columnCnt
   return tbl
end
