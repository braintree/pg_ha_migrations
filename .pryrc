require "pry-byebug"

Pry.commands.alias_command 'c', 'continue'
Pry.commands.alias_command 's', 'step'
Pry.commands.alias_command 'n', 'next'
Pry.commands.alias_command 'f', 'finish'

# https://github.com/pry/pry/issues/1275#issuecomment-131969510
# Prevent issue where text input does not display on screen in container after typing Ctrl-C in a pry repl
at_exit do
  exit!(1)
end

trap('INT') do
  begin
    Pry.run_command "continue", :show_output => true, :target => Pry.current
  rescue
    exit
  end
end
# End pry Ctrl-C workaround
