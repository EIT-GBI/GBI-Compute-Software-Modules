-- -*- lua -*-

function vardumpT(arg)
   vardump(arg.value, arg.depth, arg.key, arg.ignoreT, arg.file)
end

function vardump(value, depth, key, ignoreT, file)
   local linePrefix = ""
   local spaces     = ""
   local blank      = " "

   ignoreT = ignoreT or {}
   file    = file    or io.stdout

   if (ignoreT[key]) then return end

   if (key ~= nil) then
      linePrefix = "["..key.."] = "
   end

   if (depth == nil) then
      depth = 0
   else
      depth  = depth + 1
      spaces = blank:rep(depth*2)
   end
   if (type(value) == "table") then
      local mTable = getmetatable(value)
      if (mTable == nil) then
         file:write(spaces,linePrefix,"(table)","\n")
      else
         file:write(spaces,linePrefix,"(metatable)","\n")
         value = mTable
      end
      for k,v in pairs(value) do
         vardump(v, depth, k)
      end
   elseif (type(value) == "function" or type(value) == "thread" or 
           type(value) == "userdata" or type(value) == nil) then
      file:write(spaces,linePrefix,tostring(value),"\n")
   else
      file:write(spaces,linePrefix,"(",type(value),") ",tostring(value),"\n")
   end
end
   
