EXIT_STATUS_PERMISSION_DENIED = 100
EXIT_STATUS_NOT_FOUND = 101
EXIT_STATUS_OTHER_ERROR = 102
custom_exit_status = nil


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

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if custom_exit_status then
    return custom_exit_status
  else
    return exit_status
  end
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  local code = http_stat.statcode
--  io.stdout:write("\nServer returned status "..code.."\n")
--  io.stdout:flush()

  if code == 200 then
    local html = read_file(http_stat["local_file"])
    if html == "You do not have access to view that puush." then
      custom_exit_status = EXIT_STATUS_PERMISSION_DENIED
      return wget.actions.EXIT
    end
    return wget.actions.NORMAL
  else
    if code == 404 then
      custom_exit_status = EXIT_STATUS_NOT_FOUND
    else
      custom_exit_status = EXIT_STATUS_OTHER_ERROR
    end
    return wget.actions.EXIT
  end
end
