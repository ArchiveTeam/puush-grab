# Reads items from STDIN (one per line) and adds them to the queue.
#
# Example:
#   cat users.txt | ruby script/enqueue.rb
#

require "rubygems"
# TODO: make not hard coded
#require File.expand_path("../../app.rb", __FILE__)
require File.expand_path("/home/tracker/universal-tracker/app.rb")

slug = ARGV[0] or raise "No slug given."
env = ARGV[1] || "production"

UniversalTracker::RedisConnection.load_config(env)
redis = UniversalTracker::RedisConnection.connection
tracker_manager = UniversalTracker::TrackerManager.new(redis)
tracker = tracker_manager.tracker_for_slug(slug)

batch = []
while line = $stdin.gets
  unless line.strip.empty?
    batch << line.strip

    if batch.size >= 10000
      tracker.add_items!(batch)
      batch = []
    end
  end
end

tracker.add_items!(batch) unless batch.empty?
