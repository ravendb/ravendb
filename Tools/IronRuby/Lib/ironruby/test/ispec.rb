require 'rubygems'

unless gem 'test-spec'
  raise "Install test-spec: gem install test-spec"
end

require 'test/spec'
require 'test/unit'

# monkey-patch test-spec to not depend on ObjectSpace for finding contexts
require 'test/unit/collector/objectspace'
require 'test/unit/collector/dir'
class Test::Unit::Collector::ObjectSpace
  def collect(name=NAME)
    suite = Test::Unit::TestSuite.new(name)
    sub_suites = []
    
    # ironruby still supports ObjectSpace.each_object for classes defined with
    # the "class" keyword, which would be any test-unit testcase
    @source.each_object(Class) do |klass|
      if(Test::Unit::TestCase > klass)
        add_suite(sub_suites, klass.suite)
      end
    end
    
    # however, ironruby does not support ObjectSpace.each_object for classes
    # defined with Class.new{ }, which would be any test-spec context. However,
    # test-spec keeps track of the generated classes in Test::Spec::CONTEXTS, so
    # just pull them out of there.
    Test::Spec::CONTEXTS.each do |name, spec|
      klass = spec.testcase
      if Test::Unit::TestCase > klass
        add_suite sub_suites, klass.suite
      end
    end
    
    sort(sub_suites).each{|s| suite << s}
    suite
  end
end
class Test::Unit::Collector::Dir
  def find_test_cases(ignore=[])
    cases = []
    @object_space.each_object(Class) do |c|
      cases << c if(c < Test::Unit::TestCase && !ignore.include?(c))
    end
    Test::Spec::CONTEXTS.each do |name, spec|
      c = spec.testcase
      cases << c if(c < Test::Unit::TestCase && !ignore.include?(c))
    end
    ignore.concat(cases)
    cases
  end
end