TOPLEVEL_BINDING = binding unless defined?(TOPLEVEL_BINDING)

def repl(scope = TOPLEVEL_BINDING, msg = nil)
  Repl.start(scope, msg)
end

module Repl
  def self.start(scope = TOPLEVEL_BINDING, msg = nil)
    quitstr = ['quit', 'exit', '']
    while true
      stack = eval("caller[3..-1]", scope)
      print "\n#{stack.first}\n" if stack and not stack.empty?
      print "#{"(#{msg})" if msg}>>> "
      input = gets.strip rescue 'quit'
      break if quitstr.include?(input)
      puts "=> #{
        begin
          eval(input, scope).inspect
        rescue LoadError => le
          puts le.inspect
        rescue SyntaxError => se
          puts se.inspect
        rescue => e
          puts e.inspect
        end
      }"
    end
  end
end
