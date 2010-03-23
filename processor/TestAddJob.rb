#!/usr/bin/ruby

require 'rubygems'

require 'yaml'
require 'json'
require 'right_aws'

class JobAdder
  def initialize( access_key_id, secret_access_key )
    sqs = RightAws::SqsGen2.new access_key_id, secret_access_key
    @todo_queue = sqs.queue "UploadProcessingTodo"
    @done_queue = sqs.queue "UploadProcessingDone"
  end
  
  def add_job
    attempts = 0
    begin
      attempts += 1
      message = {
          "meta" => {
              "api_version"   => 1,
              "job_type"      => "thumbnail_generation",
              "passthru"      => {
                                  "game_id" => "14"
                              }
          },
          "source" => {
              "bucket"        => "surferbreak-development",
              "directory"     => "uploads",
              "storage_id"    => "6ef5a06cffedb5f9723ef056483eef8f"
          },
          "destination" => {
              "bucket" => "surferbreak-development",
              "directory" => "screenshots",
              "storage_id_prefix" => "6ef5a06cffedb5f9723ef056483eef8f-"
          }
      }
      send_result = @todo_queue.send_message JSON.generate(message)
      raise unless send_result
    rescue
      puts "ERROR SENDING MESSAGE. Retrying."
      retry if attempts < 3
      puts "Send message fatal failure."
    end
  end
  
  def poll_done
    while true
      message = @done_queue.pop
      if not message.nil?
        puts "Message received: #{message.body}"
        message.delete
        return
      else
        sleep 1
      end
    end
  end
end

job_adder = JobAdder.new "13WQ80HKRY1EJA7SH9R2", "67IJS5Tc8VQLrrougD2AJQBFyw3B2YER6dAHXvwj"

puts "Press enter to submit job."

while a = gets
  job_adder.add_job
  puts "Submitted job."
  job_adder.poll_done
  puts "Ready."
end