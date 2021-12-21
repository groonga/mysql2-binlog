# -*- ruby -*-

clean_white_space = lambda do |entry|
  entry.gsub(/(\A\n+|\n+\z)/, '') + "\n"
end

require_relative "lib/mysql2-replication/version"

Gem::Specification.new do |spec|
  spec.name = "mysql2-replication"
  spec.version = Mysql2Replication::VERSION
  spec.homepage = "https://github.com/groonga/mysql2-replication"
  spec.authors = ["Sutou Kouhei"]
  spec.email = ["kou@clear-code.com"]

  readme = File.read("README.md")
  readme.force_encoding("UTF-8")
  entries = readme.split(/^\#\#\s(.*)$/)
  clean_white_space.call(entries[entries.index("Description") + 1])
  description = clean_white_space.call(entries[entries.index("Description") + 1])
  spec.summary, spec.description, = description.split(/\n\n+/, 3)
  spec.license = "MIT"
  spec.files = [
    "README.md",
    "LICENSE.txt",
    "Rakefile",
    "Gemfile",
    "#{spec.name}.gemspec",
  ]
  spec.files += Dir.glob("lib/**/*.rb")
  spec.files += Dir.glob("ext/**/*.{c,rb}")
  spec.extensions = ["ext/mysql2-replication/extconf.rb"]
  Dir.chdir("bin") do
    spec.executables = Dir.glob("*")
  end

  spec.add_runtime_dependency("mysql2")
  spec.add_runtime_dependency("native-package-installer")
  spec.add_runtime_dependency("pkg-config")
end
