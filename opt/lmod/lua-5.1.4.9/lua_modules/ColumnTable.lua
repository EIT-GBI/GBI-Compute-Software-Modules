-- -*- lua -*-
require("strict")
require("capture")
local capture      = capture
local getenv       = os.getenv
local ipairs	   = ipairs
local math	   = math
local pairs	   = pairs
local print	   = print
local setmetatable = setmetatable
local stdout       = io.stdout
local string	   = string
local table        = table
local tonumber     = tonumber
local type	   = type
local prt          = io.write

ColumnTable = { gap = 3 }

local blank = ' '

module ('ColumnTable')

local function prt(...)
   stdout:write(...)
end

function new(self,t)
   local tbl
   local o = {}
   if (t.tbl) then
      tbl = t.tbl
      o   = t
   end

   setmetatable(o, self)
   self.__index  = self
   local width  = 80
   if (getenv("TERM")) then
      width  = tonumber(capture("tput cols"))
   end

   o.term_width = width
   o.tbl        = o:build_tbl(tbl)
   prt          = t.prt or prt

   return o
end

function build_tbl(self,tt)

   -- Compute Max possible number of columns
   self:number_of_columns_rows(tt)

   return tt
   --io.stderr:write("num_columns: ", o.ncols," num rows ",o.nrows,"\n")
end



function print_tbl(self)
   
   local columnCnt = {}
   for icol = 1,self.ncols do
      columnCnt[icol] = 0
   end

   local icol = 1
   local irow = 0
   local max  = math.max
   for i,v in ipairs(self.szA) do
      irow = irow + 1
      columnCnt[icol] = max(v, columnCnt[icol])
      if (irow == self.nrows) then
	 icol = icol + 1
	 irow = 0
      end
   end

   for irow = 1, self.nrows do
      local t = {}
      for icol = 1, self.ncols - 1 do
	 loc = irow + (icol - 1) * self.nrows
	 if (loc <= self.sz ) then
	    local nspaces = columnCnt[icol] - self.szA[loc]
	    t[icol] = self.tbl[loc] .. blank:rep(nspaces)
	 end
      end
      local loc = irow + (self.ncols - 1) * self.nrows
      if (loc <= self.sz ) then
	 t[self.ncols] = self.tbl[loc]
      end
      local s = table.concat(t,'')
      prt(s,"\n")
   end
end

function number_of_columns_rows(self,t)
   local max  = math.max
   local min  = math.min
   local imax = 0
   local imin = math.huge
   local gap  = self.gap

   local szA  = {}
   self.szA   = szA

   -------------------------------------------------------------------------
   -- Compute length of each entry in table t
   -------------------------------------------------------------------------
   for _,v in ipairs(t) do
      local len  = v:len() + gap
      imax = max(imax,len)
      imin = min(imin,len)
      szA[#szA + 1] = len
   end
   local sz = #t


   
   -------------------------------------------------------------------------
   -- Quit early if max width in table t is bigger than the number of
   -- columns in terminal (self.term_width)
   -------------------------------------------------------------------------
   if (imax >= self.term_width) then
      return 1
   end


   local max_columns = math.floor(min(self.term_width/imin, sz))

   local c = {}

   for ncols = 1,max_columns do
      local nrows = math.floor(sz/ncols)
      if ( nrows*ncols < sz ) then
        nrows = nrows + 1
      end

      
      local columnCnt = {}
      for icol = 1,ncols do
	 columnCnt[icol] = 0
      end
      
      local icol = 1
      local irow = 0
      for i,v in ipairs(szA) do
	 irow = irow + 1
	 columnCnt[icol] = max(v, columnCnt[icol])
	 if (irow >= nrows) then
	    icol = icol + 1
	    irow = 0
	 end
      end

      local sum = 0
      for icol = 1,ncols do
	 sum = sum + columnCnt[icol]
      end

      c[#c + 1] = (sum < self.term_width)
   end

   local ncols = 1
   for icol = max_columns, 1, -1 do
      if (c[icol]) then
	 ncols = icol
	 break
      end
   end

   local nrows = math.ceil(sz/ncols)
   self.ncols  = math.ceil(sz/nrows)
   self.nrows  = nrows
   self.sz     = sz
end
