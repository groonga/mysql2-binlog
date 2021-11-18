# -*- ruby -*-

require "rubygems"
require "bundler/gem_helper"

base_dir = File.join(__dir__)

helper = Bundler::GemHelper.new(base_dir)
def helper.version_tag
  version
end

helper.install
spec = helper.gemspec

desc "Run tests"
task :test do
  ruby("test/run.rb")
end

task default: :test
