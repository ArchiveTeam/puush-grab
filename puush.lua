read_file = function(file)
  if file then
    local f = io.open(file)
    local data = f:read(100)
    f:close()
    return data
  else
    return ""
  end
end



wget.callbacks.httploop_result = function(url, err, http_stat)
  code = http_stat.statcode
--  io.stdout:write("\nServer returned status "..code.."\n")
--  io.stdout:flush()
  
  local html = read_file(http_stat["local_file"])

  if code == 200 then
    if html == "You do not have access to view that puush." then
      return wget.actions.ABORT
    end
    return wget.actions.NORMAL
  else
    return wget.actions.ABORT
  end
end
