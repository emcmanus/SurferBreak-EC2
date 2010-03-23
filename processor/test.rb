require 'FileUtils'

@cache_path = File.expand_path(File.dirname(__FILE__)) + "/cache"

FileUtils.remove( Dir[ @cache_path + "/screenshots/fake-*" ] )

puts Dir[ @cache_path + "/screenshots/fake-*" ]