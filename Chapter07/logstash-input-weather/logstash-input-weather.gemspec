Gem::Specification.new do |s|
  s.name          = 'logstash-input-weather'
  s.version       = '0.1.0'
  s.licenses      = ['Apache License (2.0)']
  s.summary       = 'This plugin is for education purpose and to collect weather data from https://openweathermap.org'
  s.description   = 'his plugin is for education purpose and to collect weather data from https://openweathermap.org. You can provide the cities separated with comma in the configuration.'
  s.homepage      = 'https://github.com/kravigupta/logstash-input-weather'
  s.authors       = ['Ravi Kumar Gupta']
  s.email         = 'kravi.ravi@gmail.com'
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "input" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", "~> 2.0"
  s.add_runtime_dependency 'logstash-codec-plain'
  s.add_runtime_dependency 'stud', '>= 0.0.22'
  s.add_development_dependency 'logstash-devutils', '>= 0.0.16'
end
