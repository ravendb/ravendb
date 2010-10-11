version = File.read(File.expand_path("../VERSION", __FILE__)).strip

Gem::Specification.new do |spec|
	spec.platform    = Gem::Platform::RUBY
	spec.name        = "ravendb"
	spec.version     = version
	spec.files 		 = Dir['lib/**/*']
	spec.summary     = "Raven is an Open Source (with a commercial option) document database for the .NET/Windows platform."
	spec.description = <<-EOF
		Raven is an Open Source (with a commercial option) document database for the .NET/Windows platform. Raven offers a 
		flexible data model design to fit the needs of real world systems. Raven stores schema-less JSON documents, allow you 
		to define indexes using Linq queries and focus on low latency and high performance.
	EOF
	spec.author     = "Ayende Rahien"
	spec.email       = "Ayende@ayende.com"
	spec.homepage    = "http://ravendb.net/"
	spec.rubyforge_project = "ravendb"
	spec.add_dependency('json-net','= 3.5.0.52140')
end
