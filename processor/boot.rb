
def run(environment)
  include 'thumbnailGenerator'
end

Usage = <<END
  Usage: ./boot $environment
END


if ARGV.length == 1
  environment = ARGV[0]
  run environment
else
  puts Usage
end