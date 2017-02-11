# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket" # for Socket.gethostname
require 'net/http'
require 'uri'
require 'json'
require 'set'
require 'date'

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::Weather < LogStash::Inputs::Base
  config_name "weather"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # if cityNames are set, they should be used.
  config :cityName, :default => "Ahmedabad"

  # Set the key to be used with API calls
  config :key, :required => true, :default => "0fd90ff2070c6a5382e325629f219c4f"

  # Set how frequently messages should be sent.
  #
  # The default, `1`, means send a message every second.
  config :interval, :validate => :number, :default => 5

  config :jsonFields, :default => "main,clouds,sys,coord,wind"
  config :arrayFields, :default => "weather"

  public
  def register
    @host = Socket.gethostname
  end # def register

  def run(queue)
    # we can abort the loop if stop? becomes true
    while !stop?

      url = "http://api.openweathermap.org/data/2.5/weather?q="+cityName+"&appid="+key;
      #url = URI.parse(urlString)
      #http = Net::HTTP.new(url.host, url.port)

      #request = Net::HTTP::Get.new(url.request_uri)
      response = Net::HTTP.get_response(URI.parse(url))
      #response = http.request(request)
      weatherData = JSON.parse(response.body)

      event = LogStash::Event.new()

      jsonFieldsKeys = @jsonFields.split(",")
      arrayFieldsKeys = @arrayFields.split(",")
      weatherData.each do |k,v|
        if(jsonFieldsKeys.include?  k)
          v.each do |key, val|
            event.set(k.to_s + "_" +key.to_s, val)
          end
        else
          if(arrayFieldsKeys.include? k)
            v.each do |obj|
              obj.each do |key, val|
                event.set(k.to_s + "_" +key.to_s, val)
              end
            end
          end
          event.set(k.to_s, v)
        end
      end

      decorate(event)
      queue << event

      # because the sleep interval can be big, when shutdown happens
      # we want to be able to abort the sleep
      # Stud.stoppable_sleep will frequently evaluate the given block
      # and abort the sleep(@interval) if the return value is true
      Stud.stoppable_sleep(@interval) { stop? }
    end # loop
  end # def run

  # gets all venues for a given city
  def getWeatherForCity(cityName, key, queue)
  #  begin

  #  rescue
  #    puts "Some error occurred while getting weather data for City " + cityName
  #  end
  end

  def stop
    # nothing to do in this case so it is not necessary to define stop
    # examples of common "stop" tasks:
    #  * close sockets (unblocking blocking reads/accepts)
    #  * cleanup temporary files
    #  * terminate spawned threads
  end
end # class LogStash::Inputs::Weather
