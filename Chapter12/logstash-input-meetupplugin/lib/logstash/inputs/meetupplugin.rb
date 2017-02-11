# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/timestamp"
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

class LogStash::Inputs::Meetupplugin < LogStash::Inputs::Base
  config_name "meetupplugin"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # The default, `900`, means send a message every 15 seconds.
  config :interval, :validate => :number, :default => 900
  # Set the key to be used with API calls
  config :key, :required => true, :default => ""

  # Taking user's set country code
  config :countryCode, :required => true, :default => "IN"

  # if event ids are set, they should be used.
  config :cityNames, :default => ""

  config :enableTopics, :validate => :boolean, :default => false
  config :enableCategories, :validate => :boolean, :default => false
  config :enableVenues, :validate => :boolean, :default => false
  config :enableGroups, :validate => :boolean, :default => false
  config :enableMeetup, :validate => :boolean, :default => true
  config :citiesCount, :validate => :number, :default => 5

  config :numberFields, :default => "yes_rsvp_count,waitlist_count,maybe_rsvp_count,headcount,duration,distance,rating,rating_count,members,lat,lon,utc_offset"
  config :setMessage, :valudate => :number, :default => 0
  public
  def register
    @host = Socket.gethostname
  end # def register

  def run(queue)

    printAllConfig()

    if(@countryCode.length > 0)
      puts "Got countryCode - " + @countryCode + " and cityNames - " + @cityNames

      @topics = Set.new
      if(@cityNames.length > 0)
        cities = @cityNames.split(",")
      else
        cities = getCities(countryCode).to_a
      end

      # we can abort the loop if stop? becomes true
      while !stop?
        if(@enableMeetup == true)
          cityCounter = 0

          puts "Getting all events for cities"
          while(cityCounter < cities.length)
            cityNameToken = cities[cityCounter].downcase.tr(" ", "+").split(":")
            cityName = cityNameToken[0]
            stateCode = ""
            if(cityNameToken.length == 2)
              stateCode = cityNameToken[1]
            end
            getEventsByCity(@countryCode.downcase, cityName, stateCode, @key, queue)
            cityCounter = cityCounter + 1
          end
        end
        if(@enableGroups == true)
          # getting all groups
          cityCounter = 0
          puts "Getting all groups for cities"
          while(cityCounter < cities.length)
            cityNameToken = cities[cityCounter].downcase.tr(" ", "+").split(":")
            cityName = cityNameToken[0]
            stateCode = ""
            if(cityNameToken.length == 2)
              stateCode = cityNameToken[1]
            end
            getGroupsByCity(@countryCode.downcase, cityName, stateCode, @key, queue)
            cityCounter = cityCounter + 1
          end
        end
        if(@enableCategories == true)
          # All categories
          puts "Getting all categories"
          getCategories(@key, queue)
        end
        if(@enableVenues == true)
          # All venues by captured cities
          cityCounter = 0
          puts "Gettng all venues for cities"
          while(cityCounter < cities.length)
            cityNameToken = cities[cityCounter].downcase.tr(" ", "+").split(":")
            cityName = cityNameToken[0]
            stateCode = ""
            if(cityNameToken.length == 2)
              stateCode = cityNameToken[1]
            end
            getVenuesByCity(@countryCode.downcase, cityName, stateCode, @key, queue)
            cityCounter = cityCounter + 1
          end
        end
        if(@enableTopics == true)
          # All topics
          puts "Getting all topics"
          @topics.each do |topic|
            getTopics(topic, @key, queue)
            Stud.stoppable_sleep(2) { stop? } # sleeping for 2 seconds
          end
        end
        puts "Got everything once, Now will try after some time."

        # because the sleep interval can be big, when shutdown happens
        # we want to be able to abort the sleep
        # Stud.stoppable_sleep will frequently evaluate the given block
        # and abort the sleep(@interval) if the return value is true
        Stud.stoppable_sleep(@interval) { stop? }
      end # loop
    else
      puts "Please provide a country code and city names (optional)."
    end
  end # def run

  # gets all venues for a given city
  def getVenuesByCity(countryCode, cityName, stateCode, key, queue)
    state = ""
    if(stateCode.length > 0)
      state = "&state=" + stateCode
    end
    begin
      urlString = "https://api.meetup.com/2/open_venues?country="+countryCode+"&city="+cityName+state+"&key="+key+"&sign=true";
      url = URI.parse(urlString)
      http = Net::HTTP.new(url.host, url.port)
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_NONE

      request = Net::HTTP::Get.new(url.request_uri)

      response = http.request(request)
      puts "Got Venue response for City " + cityName.to_s
      json = JSON.parse(response.body)

      json["results"].each do |venue|
          event = prepareEventForVenue(venue, countryCode, cityName, stateCode)
          queue << event
      end
    rescue
      puts "Some error occurred while getting venue data for City " + cityName  +" Country " + countryCode
    end
  end

  def prepareEventForVenue(venue, countryCode, cityName, stateCode)
    our_venue_id = countryCode + "_venue_id_" + venue["id"].to_s
    event = LogStash::Event.new("venue_id" => our_venue_id)
    if(@setMessage == 1)
      event.set("message", venue)
    end
    numberFieldsKeys = @numberFields.split(",")
    venue.each do |k,v|
      if(numberFieldsKeys.include? k)
        event.set(k.to_s, v)
      else
        event.set(k.to_s, v.to_s)
      end
    end
    #event.set("[@{METADATA}][_id]", our_meetup_id)
    #event.set("[document_id]", our_meetup_id)
    event.set("document_id", our_venue_id)
    event.set("meetup_data_type", "venue")
    event.set("venue_lat", venue["lat"].to_f)
    event.set("venue_lon", venue["lon"].to_f)

    #event.set("venue_lon_lat", [venue['lon'],venue['lat']])
    event.set("location", {"lat" => venue['lat'].to_f, "lon" => venue['lon'].to_f})

    # Setting Country
    address = venue["address_1"].to_s + ", " +venue["address_2"].to_s + ", "+cityName + ", " +countryCode
    event.set("venue_address", address)
    event.set("venue_id", countryCode + "_venue_id_" + venue["id"].to_s)

    event.set("country", countryCode)
    event.set("city_id", countryCode + "_city_id_" + cityName)
    event.set("state", stateCode)
    event.set("city", cityName.tr("+", " "))
    decorate(event)
    return event
  end

  # gets all topics
  def getCategories(key, queue)
    begin
      urlString = "https://api.meetup.com/2/categories?key="+key+"&sign=true";
      url = URI.parse(urlString)
      http = Net::HTTP.new(url.host, url.port)
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_NONE

      request = Net::HTTP::Get.new(url.request_uri)

      response = http.request(request)
      json = JSON.parse(response.body)

      json["results"].each do |category|
          event = prepareEventForCategory(category)
          queue << event
      end
    rescue
      puts "Some error occurred while getting category data "
    end
  end

  def prepareEventForCategory(category)
    our_category_id = "category_id_" + category["id"].to_s
    event = LogStash::Event.new("category_id" => our_category_id)
    if(@setMessage == 1)
      event.set("message", category)
    end
    category.each do |k,v|
      event.set(k.to_s, v.to_s)
    end
    event.set("document_id", our_category_id)
    event.set("meetup_data_type", "category")
    decorate(event)
    return event
  end

  # gets all categories
  def getTopics(topic, key, queue)
    begin
      urlString = "https://api.meetup.com/topics?topic="+topic+"&key="+key+"&sign=true";
      url = URI.parse(urlString)
      http = Net::HTTP.new(url.host, url.port)
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_NONE

      request = Net::HTTP::Get.new(url.request_uri)

      response = http.request(request)
      json = JSON.parse(response.body)

      json["results"].each do |topic|
          event = prepareEventForTopic(topic)
          queue << event
      end
    rescue
      puts "Some error occurred while getting topic data "
    end
  end

  def prepareEventForTopic(topic)
    our_topic_id = "topic_id_" + topic["id"].to_s
    event = LogStash::Event.new("topic_id" => our_topic_id)
    if(@setMessage == 1)
      event.set("message", topic)
    end
    topic.each do |k,v|
      event.set(k.to_s, v.to_s)
    end
    event.set("members", topic["members"].to_i)
    event.set("document_id", our_topic_id)
    event.set("meetup_data_type", "topic")
    decorate(event)
    return event
  end

  # Gets open events for a given country and city. The event is queued to given queue.
  def getGroupsByCity(countryCode, cityName, stateCode, key, queue)
    state = ""
    if(stateCode.length > 0)
      state = "&state=" + stateCode
    end
    begin
      url = URI.parse("https://api.meetup.com/2/groups?country="+countryCode+"&city="+cityName+state+"&key="+key+"&sign=true")
      #puts "Calling URL " + eventsByCountryCityURL
      #response = Net::HTTP.get_response(URI.parse(eventsByCountryCityURL))
      http = Net::HTTP.new(url.host, url.port)
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_NONE

      request = Net::HTTP::Get.new(url.request_uri)

      response = http.request(request)
      puts "Got Group response for City " + cityName.to_s
      json = JSON.parse(response.body)

      json["results"].each do |group|
          #puts "preparing event for  - " + meetup["name"]
          event = prepareEventForGroup(group, countryCode, cityName, stateCode)
          queue << event
      end
    rescue => e
      puts "Some error occurred while getting data for City " + cityName  +" Country " + countryCode
      puts "caught exception #{e}! "
    end
  end

  def prepareEventForGroup(group, countryCode, cityName, stateCode)
    our_group_id = countryCode + "_meetup_group_id_" + group["id"].to_s
    event = LogStash::Event.new("meetup_group_id" => our_group_id)
    if(@setMessage == 1)
      event.set("message", group)
    end
    numberFieldsKeys = @numberFields.split(",")
    group.each do |k,v|
      if(numberFieldsKeys.include? k)
        event.set(k.to_s, v)
      else
        event.set(k.to_s, v.to_s)
      end
    end

    event.set("rating", group["rating"].to_f)
    event.set("meetup_data_type", "group")

    groupTopics = Array.new

    # Get all topics and add to the set
    if(group["topics"].to_s.length > 0)
      group["topics"].each do |topic|
        @topics.add(topic["urlkey"])
        groupTopics << topic["urlkey"]
      end
    end

    #puts "Group Topics are "
    #puts groupTopics

    event.set("topicsURLKeys", groupTopics)

    # Get the category
    if(group["category"].to_s.length > 0)
      event.set("category_id", "category_id_" + group["category"]["id"].to_s)
      event.set("category_name", group["category"]["name"].to_s)
      event.set("category_short_name", group["category"]["shortname"].to_s)
    end

    event.set("document_id", our_group_id)

    event.set("country", countryCode)
    event.set("city_id", countryCode + "_city_id_" + cityName)
    event.set("city", cityName.tr("+", " "))
    event.set("group_id", countryCode + "_group_id_" + group["id"].to_s)
    event.set("state", stateCode)

    #event.set("group_lon_lat", [group['lon'],group['lat']])
    event.set("location", {"lat" => group['lat'].to_f, "lon" => group['lon'].to_f})

    utc_offset = group["utc_offset"].to_i
    createdTime = group['created'].to_i - utc_offset
    event.set("created", LogStash::Timestamp.at(createdTime / 1000, (createdTime % 1000) * 1000))

    decorate(event)
    return event
  end

  # Gets open events for a given country and city. The event is queued to given queue.
  def getEventsByCity(countryCode, cityName, stateCode, key, queue)
    #begin
      state = ""
      if(stateCode.length > 0)
        state = "&state=" + stateCode
      end
      eventsByCountryCityURL = URI.parse("https://api.meetup.com/2/open_events?country="+countryCode+"&city="+cityName+state+"&key="+key+"&sign=true")
      #puts "Calling URL " + eventsByCountryCityURL
      #response = Net::HTTP.get_response(URI.parse(eventsByCountryCityURL))
      http = Net::HTTP.new(eventsByCountryCityURL.host, eventsByCountryCityURL.port)
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_NONE

      request = Net::HTTP::Get.new(eventsByCountryCityURL.request_uri)

      response = http.request(request)
      puts "Got response for City " + cityName.to_s
      json = JSON.parse(response.body)

      json["results"].each do |meetup|
          #puts "preparing event for  - " + meetup["name"]
          event = prepareEventForMeetup(meetup, countryCode, cityName, stateCode)
          queue << event
      end
    #rescue
      #puts "Some error occurred while getting data for City " + cityName  +" Country " + countryCode
    #end
  end

  def prepareEventForMeetup(meetup, countryCode, cityName, stateCode)
    our_meetup_id = countryCode + "_meetup_id_" + meetup["id"].to_s
    event = LogStash::Event.new("meetup_id" => our_meetup_id)
    if(@setMessage == 1)
      event.set("message", meetup)
    end
    numberFieldsKeys = @numberFields.split(",")
    meetup.each do |k,v|
      if(numberFieldsKeys.include? k)
        event.set(k.to_s, v)
      else
        event.set(k.to_s, v.to_s)
      end
    end
    #event.set("[@{METADATA}][_id]", our_meetup_id)
    #event.set("[document_id]", our_meetup_id)
    event.set("document_id", our_meetup_id)
    event.set("meetup_data_type", "meetup")

    utc_offset = meetup["utc_offset"].to_i

    createdTime = meetup['created'].to_i - utc_offset
    eventTime = meetup['time'].to_i - utc_offset
    updatedTime = meetup['updated'].to_i - utc_offset

    event.set("created", LogStash::Timestamp.at(createdTime / 1000, (createdTime % 1000) * 1000))
    event.set("time", LogStash::Timestamp.at(eventTime / 1000, (eventTime % 1000) * 1000))
    event.set("updated", LogStash::Timestamp.at(updatedTime / 1000, (updatedTime % 1000) * 1000))

    duration = meetup["duration"].to_i
    #event.set("durationMinutes", duration/(1000 * 60))

    if(meetup["venue"].to_s.length == 0)
      # Setting Country
      event.set("address", "")
      event.set("venue_id", "")
      # storing lat lon from goup in case venue is missing.
      event.set("lon", meetup["group"]["group_lon"].to_f)
      event.set("lat", meetup["group"]["group_lat"].to_f)
      #event.set("meetup_lon_lat", [meetup['group']['group_lon'],meetup['group']['group_lat']])
      event.set("location", { "lat" => meetup['group']['group_lat'].to_f, "lon" => meetup['group']['group_lon'].to_f})
    else
      # Setting Country
      address = meetup["venue"]["name"].to_s + ", " +meetup["venue"]["address_1"].to_s + ", "+cityName + ", " +countryCode
      event.set("address", address)
      event.set("venue_id", countryCode + "_venue_id_" + meetup["venue"]["id"].to_s)
      event.set("lat", meetup["venue"]["lat"].to_f)
      event.set("lon", meetup["venue"]["lon"].to_f)
      #event.set("meetup_lon_lat", [meetup['venue']['lon'],meetup['venue']['lat']])
      event.set("location", {"lat" => meetup['venue']['lat'].to_f, "lon" =>  meetup['venue']['lon'].to_f})
    end

    event.set("country", countryCode)
    event.set("city_id", countryCode + "_city_id_" + cityName)
    event.set("city", cityName.tr("+", " "))
    event.set("state", stateCode)
    event.set("group_id", countryCode + "_group_id_" + meetup["group"]["id"].to_s)
    event.set("group_name", countryCode + "_group_name_" + meetup["group"]["name"].to_s)
    event.set("group_lon", meetup["group"]["group_lon"].to_f)
    event.set("group_lat", meetup["group"]["group_lat"].to_f)

    decorate(event)
    return event
  end

  def getCities(countryCode)

    citiesSet = Set.new

    #url = URI.parse("https://api.meetup.com/2/open_events?country="+countryCode+"&city="+cityName+"&key="+key+"&sign=true")
    url = URI.parse("https://api.meetup.com/2/cities?country="+countryCode+"&page=200")
    #puts "Calling URL " + eventsByCountryCityURL
    #response = Net::HTTP.get_response(URI.parse(eventsByCountryCityURL))
    http = Net::HTTP.new(url.host, url.port)
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE

    request = Net::HTTP::Get.new(url.request_uri)

    response = http.request(request)
    #puts response.body
    json = JSON.parse(response.body)

    json["results"].each do |city|
        #puts "Got city as " + city["city"].to_s
        if(@citiesCount > citiesSet.length)
          if(city["state"].to_s.length >0)
            cityNameT = city["city"].to_s + ":" +city["state"].to_s
            citiesSet.add(cityNameT)
          else
            citiesSet.add(city["city"].to_s)
          end
        end
    end
    return citiesSet
  end

  def printAllConfig()
    puts "======================================================================"
    puts "Got configurations for Meetup plugins as -  "
    puts "Country - " + @countryCode.to_s
    puts "Cities - " + @cityNames.to_s
    puts "Interval by which meetup data should be crawled - " + @interval.to_s
    puts "Key - " + "Its a secret :D"
    puts "enableTopics - " + @enableTopics.to_s
    puts "enableVenues - " + @enableVenues.to_s
    puts "enableGroups - " + @enableGroups.to_s
    puts "enableCategories - " + @enableCategories.to_s
    puts "enableMeetup - " + @enableMeetup.to_s
    puts "That's all for configurations."
    puts "======================================================================"
  end

  def stop
    # nothing to do in this case so it is not necessary to define stop
    # examples of common "stop" tasks:
    #  * close sockets (unblocking blocking reads/accepts)
    #  * cleanup temporary files
    #  * terminate spawned threads
  end
end # class LogStash::Inputs::Meetupplugin
