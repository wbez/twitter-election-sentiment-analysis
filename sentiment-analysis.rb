#!/usr/bin/env ruby
require 'rubygems'
require 'twitter'
require 'pp'
require 'xlsx_writer'
require 'fileutils'
require 'geocoder'
require 'net/http'

MAX_ATTEMPTS = 10

start = Time.now

doc = XlsxWriter.new



Twitter.configure do |config|
  config.consumer_key = 'aTvMTvc7IuShZ3KOyuQ'
  config.consumer_secret = 'r2iHsycen9HpjncRFJh2hYfxS7ChaW51U1HUw6rM'
  config.oauth_token = '6429542-SCj7hyOcjgei5rPRSoSiHm1DKWm9xNTlOyAMM8oxjq'
  config.oauth_token_secret = '6vnKTWPKJX4KN27kHjUcGu5yV3opnzI1XtGlSsKBFg'
end

# District 2 – keywords – Jesse Jackson Jr. (Jackson) vs Brian Woodworth (woodworth)
# District 8 – keywords – Joe Walsh (walsh) vs Tammy Duckworth (duckworth)
# District 10 – Keywords – Robert Dold (dold) vs Brad Schneider (Schneider)
# District 11 – Judy Biggert (biggert) vs Bill Foster (foster)
 
# Indiana Senate Race – Joe Donnelly (Donnelly) vs Richard Mourdock (mourdock, said with a long roll as if thunder is cracking when you say his name)
 
# Presidential election: Barack Obama (Obama) vs Mitt Romney (Romney)
 
# Other keywords: (vote, voting, voted, ballot, election, electoral college, popular vote

searches = ["jesse jackson OR woodworth -filter:retweets -http", 
	"joe walsh OR duckworth -filter:retweets -http", 
	"dold OR schneider -maddy_schneider -filter:retweets -http", 
	"biggert OR bill foster -filter:retweets -http", 
	"donnelly OR mourdock -filter:retweets -http", 
	"obama OR romney -filter:retweets -http", 
	"vote OR voting OR voted OR ballot OR election OR electoral college OR popular vote -filter:retweets -http"
]
index = 0
searches.each do |search|
	since_id = 0
	max_id = 0
	index = index + 1
	if index == 1
		sheet = doc.add_sheet("District 2")
		since_id = 265974916539621000+1
	elsif index == 2
		sheet = doc.add_sheet("District 8")
		since_id = 265955382290182000+1
	elsif index == 3
		sheet = doc.add_sheet("District 10")
		since_id = 265969765217104000+1
	elsif index == 4
		sheet = doc.add_sheet("District 11")
		since_id = 265974422505136000+1
	elsif index == 5
		sheet = doc.add_sheet("Indiana Senate")
		since_id = 265975208865829000+1
	elsif index == 6
		sheet = doc.add_sheet("Prez")
		since_id = 265976815053578000+1
	elsif index == 7
		sheet = doc.add_sheet("Voting")
		since_id = 265984784684167000+1
	end

	sheet.freeze_top_left = 'A2'
	sheet.add_row([
	  "Tweet ID",
	  "User",
	  "Tweet",
	  "Created At",
	  "Followers",
	  "Location",
	  "Coordinates",
	  "Address",
	  "Polarity"
	])
	set = 1
	how_many = 0
	rows = []
	sentiment_json = {"data" => []}
	while set <= 5 && (how_many%100 == 0 || how_many == 0)
		pp "set: #{set}, how_many = #{how_many}, since_id = #{since_id}, max_id = #{max_id}, search = #{search}"
		if set == 1
			results = Twitter.search(search, :count => 100, :geocode => '41.743507,-88.011847,75mi', :result_type => "recent", :since_id => since_id).results
		else
			results = Twitter.search(search, :count => 100, :geocode => '41.743507,-88.011847,75mi', :result_type => "recent", :max_id => max_id).results
		end
		set += 1
		first = true
		if !results.nil?
			how_many = -1
			count = 0
			results.map do |status|
				num_attempts = 0
				count += 1
				begin
					how_many += 1
					pp "#{how_many} | #{count}"
					num_attempts += 1
					text = status.text
					tweeter = status.from_user
					id = status.id
					max_id = id
					if first
						since_id = id
						first = false
						how_many += 1
						pp "#{how_many} | #{count}"
					end
					geo = status.geo
					coordinates = ""
					address = ""
					if geo
						lat = geo.latitude
						long = geo.longitude

						address = Geocoder.address([lat, long])
						coordinates = "#{lat}, #{long}"
					end
					created_at = status.created_at.to_s
					pp "user search"
					user = Twitter.user_search(tweeter).first
					pp "end user search"
					if user
						followers = user.followers_count
						location = user.location
					end

					status_json = {"id" => id, "text" => text}
					sentiment_json['data'].push(status_json)

					pp "#{text} | #{tweeter} | #{id} | #{location} | #{lat},#{long} | #{address} | #{created_at} | #{followers} | #{created_at}"
					hash = {:id => id, :tweeter => tweeter, :text => text, :created_at => created_at, :followers => followers, :location => location, :coordinates => coordinates, :address => address}
					rows.push(hash)
					# sheet.add_row([
					# 	{:type => :BigDecimal, :value => id},
					# 	tweeter,
					# 	text,
					# 	created_at,
					# 	followers,
					# 	location,
					# 	coordinates,
					# 	address
					# ])
				rescue Twitter::Error::TooManyRequests => error
					pp "too many requests: #{num_attempts}"

					how_many -= 1

					if num_attempts <= MAX_ATTEMPTS
					    # NOTE: Your process could go to sleep for up to 15 minutes but if you
					    # retry any sooner, it will almost certainly fail with the same exception.
					    sleep error.rate_limit.reset_in
					    retry
					else
						raise
					end
				rescue
					pp "other error"
				end
			end
		else
			how_many = -1	
		end
		pp "set: #{set}, how_many = #{how_many}, since_id = #{since_id}, max_id = #{max_id}, search = #{search}"
	end
	
	uri = URI.parse("http://www.sentiment140.com/api/bulkClassifyJson?appid=webmaster@wbez.org")		
	req = Net::HTTP::Post.new(uri.request_uri, initheader = {'Content-Type' =>'application/json'})
	req.body = sentiment_json.to_json
	response = Net::HTTP.new(uri.host, uri.port).start {|http| http.request(req) }
	response_data = JSON.parse response.body
	sentiment_data = {}
	response_data["data"].each do |data|
		response_id = data['id']
		polarity = data['polarity']
		sentiment_data.merge! response_id => polarity
	end		

	rows.each do |row|
		sheet.add_row([
			{:type => :BigDecimal, :value => row[:id]},
			row[:tweeter],
			row[:text],
			row[:created_at],
			row[:followers],
			row[:location],
			row[:coordinates],
			row[:address],
			sentiment_data[row[:id]]
		])
	end
	
end

::FileUtils.mv doc.path, 'sentiment-9.xlsx'

doc.cleanup

done = Time.now

pp start
pp done
pp done - start