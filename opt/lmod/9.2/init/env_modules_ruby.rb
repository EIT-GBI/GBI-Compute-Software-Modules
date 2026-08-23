class ENVModule
  def ENVModule.module(*args)
    if args[0].kind_of?(Array) then
      args = args[0].join(' ')
    else
      args = args.join(' ')
    end
    eval `/Users/johannes.blaschke/Developer/cli-util-mods/opt/lmod/lmod/libexec/lmod ruby #{args}`
    return true
  end
end

      
