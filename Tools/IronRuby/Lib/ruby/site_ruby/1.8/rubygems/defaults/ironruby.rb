#Tests are loading this file currently, so this should block it.
unless $utr_runner
  module Gem
    def self.default_exec_format
      exec_format = ConfigMap[:ruby_install_name].sub('ir', '%s') rescue '%s'

      unless exec_format =~ /%s/ then
        raise Gem::Exception,
          "[BUG] invalid exec_format #{exec_format.inspect}, no %s"
      end

      exec_format
    end

    def self.platforms
      [
        Gem::Platform::RUBY,
        Gem::Platform.new('universal-dotnet'),
        Gem::Platform.local,
        Gem::Platform.new('universal-unknown')
      ]
    end
  end
end
