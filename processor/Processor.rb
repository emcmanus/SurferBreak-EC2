#!/usr/bin/ruby

# This is a simple SQS poller
# 
# When a new job is received, we'll remove it from the TODO queue, generate the thumbnails,
# POST the thumbnails to S3, and place a job finished message on the DONE queue.
# 
# The poller will loop every 2 seconds, but will immediately check for new jobs upon completion
# of the first. The cost for queries will be about $1.30/server/mo.


require 'rubygems'

require 'yaml'
require 'json'
require 'right_aws'
require 'curb'
require 'timeout'
require 'daemons'



debug = false
environment = "production"

for i in (0...(ARGV.count)) do
  if ARGV[i].include? "debug"
    debug = true
  elsif ARGV[i].include? "environment"
    environment = ARGV[i+1]
    i += 1
  elsif ARGV[i].include? "help"
    puts "Usage: ./Processor.rb [--debug] [--environment development]\n"
    puts "Debug runs processor as a non-daemon process, logging to STDOUT."
    puts "Environment specifies which parameters to pick from the config. (Currently ignored.)"
    exit
  end
end

# Get path to log file before daemonizing
log_file = File.expand_path(File.dirname(__FILE__)).to_s + "/processor_#{environment}.log"

if debug
  logger = Logger.new STDOUT
  logger.info "Starting in debug mode."
else
  puts "Starting daemon."
  Daemons.daemonize
  logger = Logger.new log_file, 10, 1024000
  logger.info "Poller started."
end

logger.warn "Poller currently ignores the environment, the API keys are hardcoded."


# Constants

MAX_API_VERSION   = 1
JOB_TYPE          = "thumbnail_generation"


class Processor
  def initialize( access_key_id, secret_access_key )
    sqs = RightAws::SqsGen2.new access_key_id, secret_access_key
    @todo_queue = sqs.queue "UploadProcessingTodo"
    @done_queue = sqs.queue "UploadProcessingDone"
    
    @s3 = RightAws::S3.new access_key_id, secret_access_key
    
    @access_key_id = access_key_id
    @secret_access_key = secret_access_key
    
    @root_path  = File.expand_path(File.dirname(__FILE__))
    @cache_path = @root_path + "/cache"
    @screen_grabber = @root_path + "/bin/screen_grabber"
  end
  
  def start
    while true
      @job = @todo_queue.pop
      if not @job.nil?
        process
      else
        sleep 2
      end
    end
  end
  
  def recover
    # We'll try to send a failure message
    begin
      job = JSON.parse @job.body
    rescue JSON::ParserError, RuntimeError
      job = { "meta" => { "passthru" => "Bad JSON Request." } }
    end
    job_receipt = {
        "meta" => {
            "api_version"   => 1,
            "job_type"      => "thumbnail_generation",
            "success"       => false,
            "error_message" => "Fatal error",
            "passthru"      => job['meta']['passthru']
        },
        "screenshots" => {
            "width"         => 0,
            "height"        => 0,
            "platform"      => "",
            "bucket"        => "",
            "directory"     => "",
            "default"       => "",
            "storage_ids"   => []
        }
    }
    attempts = 0
    begin
      attempts += 1
      send_success = @done_queue.send_message JSON.generate( job_receipt )
      raise unless send_success
    rescue
      retry if attempts < 3
    end
  end
  
  private
    
    def process
      
      begin
        job = JSON.parse @job.body
      rescue JSON::ParserError
        raise
      end
      
      logger.info "Received job: #{@job.body}"
      
      raise if job["meta"]["api_version"] > MAX_API_VERSION
      raise if job["meta"]["job_type"] != JOB_TYPE
      
      # Defaults
      error_message = ""
      default_thumb = ""
      error = false
      attempts = 0
      
      # File paths
      remote_path = "http://#{job['source']['bucket']}.s3.amazonaws.com/#{job['source']['directory']}/#{job['source']['storage_id']}"
      local_path = @cache_path + "/roms/#{job['source']['storage_id']}"
      screenshots_prefix = @cache_path + "/screenshots/#{job['destination']['storage_id_prefix']}"
      
      # Cleanup any previous attempts
      cleanup local_path, screenshots_prefix
      
      # Download ROM
      begin
        attempts += 1
        Curl::Easy.download remote_path, local_path
      rescue Curl::Err
        retry unless attempts > 3
        error_message = "Curl failed to download ROM from S3."
        error = true
      end
      
      # Thumbnail generation - safely kill if there's an issue
      if File.exists? local_path
        pipe = IO.popen("#{@screen_grabber} #{local_path} #{screenshots_prefix}")
        pipe_pid = pipe.pid
        begin
          timeout(15){
            default_thumb = pipe.read
            default_thumb.strip!
          }
        rescue Timeout::Error
          Process.kill 'TERM', pipe_pid
          error_message = "Thumbnail generation timed out."
          default_thumb = ""
          error = true
          if Dir[ screenshots_prefix + "*" ].length > 0 # Let's assume the last screenshot is corrupt
            last_screenshot = Dir[ screenshots_prefix + "*" ].last
            FileUtils.remove(last_screenshot)
          end
        end
      end
      
      screenshot_bucket = @s3.bucket job['destination']['bucket'], true, 'public-read'
      
      screenshot_files = Dir[ screenshots_prefix + "*" ]
      
      # Upload screenshots to S3
      if not screenshot_files.nil? and screenshot_files.length > 0
        screenshot_files.each do | screenshot_path |
          attempts = 0
          remote_id = screenshot_path.split(/\//).last
          begin
            attempts += 1
            key = screenshot_bucket.key "#{job['destination']['directory']}/#{remote_id}"
            put_result = key.put open(screenshot_path), 'public-read'
            raise if put_result == false
          rescue RuntimeError => e
            retry unless attempts > 3
            error = true
            error_message = "Thumbnail upload failed."
          end
        end
      end
      
      # Receipt parameters
      default_thumb = default_thumb.split(/\//).last unless error or default_thumb.nil?
      job_success = !error
      screenshot_ids = []
      
      screenshot_files.each do | path |
        screenshot_ids.push path.split(/\//).last
      end
      
      job_receipt = {
          "meta" => {
              "api_version"   => 1,
              "job_type"      => "thumbnail_generation",
              "success"       => job_success,
              "error_message" => error_message,
              "passthru"      => job['meta']['passthru']
          },
          "screenshots" => {
              "width"         => 256,
              "height"        => 224,
              "platform"      => "SNES",
              "bucket"        => job['destination']['bucket'],
              "directory"     => job['destination']['directory'],
              "default"       => default_thumb,
              "storage_ids"   => screenshot_ids
          }
      }
      
      attempts = 0
      begin
        attempts += 1
        send_success = @done_queue.send_message JSON.generate( job_receipt )
        raise unless send_success
      rescue
        retry if attempts < 3
      end
      
      if not error_message.empty?
        logger.error error_message
      end
      
      cleanup local_path, screenshots_prefix
      
      logger.info "Finished job."
    end
    
    
    def clean_by_prefix( prefix )
      FileUtils.remove( Dir[ prefix + "*" ] ) unless prefix.include? "../"
    end
    
    
    def cleanup( local_path, screenshots_prefix )
      unless local_path.include? "../"
        FileUtils.remove(local_path) if File.exists?(local_path)
        clean_by_prefix( screenshots_prefix )
      end
    end 
    
end

processor = Processor.new "13WQ80HKRY1EJA7SH9R2", "67IJS5Tc8VQLrrougD2AJQBFyw3B2YER6dAHXvwj"

begin
  processor.start
rescue Exception => e
  processor.recover
  logger.warn "Recovered from error: #{e.inspect}\n#{$@}\n#{e.backtrace}"
  logger.warn "Restarting loop."
  retry
end
