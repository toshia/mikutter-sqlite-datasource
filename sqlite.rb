# -*- coding: utf-8 -*-

require 'sequel'

module Plugin::SQLite
  class DataSource
    include Retriever::DataSource
    FileUtils.mkdir_p(File.join(Environment::CACHE, 'sqlite'))
    @@db = Sequel.sqlite(File.join(Environment::CACHE, 'sqlite', 'database.db'))
    @@db[<<SQL].to_a
CREATE TABLE IF NOT EXISTS `messages` (
  `id` integer NOT NULL PRIMARY KEY,
  `user_id` integer default NULL,
  `message` text NOT NULL,
  `receiver_id` integer default NULL,
  `replyto_id` integer default NULL,
  `retweet_id` integer default NULL,
  `source` text,
  `geo` text,
  `exact` integer default 0,
  `modified` integer NOT NULL,
  `created` integer NOT NULL,
  PRIMARY KEY  (`id`)
);
SQL
    @@db[<<SQL].to_a
CREATE TABLE IF NOT EXISTS `users` (
  `id` integer NOT NULL,
  `idname` text NOT NULL,
  `name` text,
  `location` text,
  `detail` text,
  `profile_image_url` text,
  `url` text,
  `protected` integer default 0,
  `followers_count` integer default NULL,
  `friends_count` integer default NULL,
  `statuses_count` integer default NULL,
  `favourites_count` integer default NULL,
  `created` integer NOT NULL,
  PRIMARY KEY  (`id`));
SQL
    @@db[<<SQL].to_a
CREATE TABLE IF NOT EXISTS `favorites` (
  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT,
  `message_id` integer NOT NULL,
  `user_id` integer NOT NULL,
  `created` integer NOT NULL
);
SQL

    def self.db
      @@db end

    def table
      @@db[@table_name] end

   def findbyid(id)
     if id.is_a? Array or id.is_a? Set
       table.filter(@primary_key => id).map(&method(:unserialize_record)).to_a
     else
       table.filter(@primary_key => id).map(&method(:unserialize_record)).first end
   rescue SQLite3::Exception, Sequel::Error => e
     error e
     nil end

   def store_datum(datum)
     record = table.filter(@primary_key => datum[@primary_key])
     if record
       record.update serialize_record(datum)
     else
       table << serialize_record(datum)
     end
     true
   rescue SQLite3::Exception, Sequel::Error => e
     error e
     false end

   def unserialize_record(record)
     record end
  end

  class MessageDataSource < DataSource

    def initialize
      super
      @table_name = :messages
      @primary_key = :id end

    def serialize_record(datum)
      { id: datum[:id],
        user_id: datum[:user],
        message: datum[:message],
        receiver_id: datum[:receiver],
        replyto_id: datum[:replyto],
        retweet_id: datum[:retweet],
        source: datum[:source],
        geo: datum[:geo],
        exact: datum[:exact] ? 1 : 0,
        modified: (datum[:modified] || datum[:created]).to_i,
        created: datum[:created].to_i } end

    def unserialize_record(record)
      result = {
        id: record[:id],
        user: User.findbyid(record[:user_id]),
        message: record[:message],
        source: record[:source],
        geo: record[:geo],
        exact: record[:exact] != 0,
        modified: Time.at(record[:modified]),
        created: Time.at(record[:created])
      }
      result[:receiver] = User.findbyid(record[:receiver_id]) if record[:receiver_id]
      result[:replyto] = Message.findbyid(record[:replyto_id]) if record[:replyto_id]
      result[:retweet] = Message.findbyid(record[:retweet_id]) if record[:retweet_id]
      result end

    Message.add_data_retriever(new)
  end

  class UserDataSource < DataSource

    def initialize
      super
      @table_name = :users
      @primary_key = :id end

    def serialize_record(datum)
      { id: datum[:id],
        idname: datum[:idname],
        name: datum[:name],
        location: datum[:location],
        detail: datum[:detail],
        profile_image_url: datum[:profile_image_url],
        url: datum[:url],
        protected: datum[:protected] ? 1 : 0,
        followers_count: datum[:followers_count],
        friends_count: datum[:friends_count],
        statuses_count: datum[:statuses_count],
        favourites_count: datum[:favourites_count],
        created: datum[:created].to_i } end

    def unserialize_record(record)
      { id: record[:id],
        idname: record[:idname],
        name: record[:name],
        location: record[:location],
        detail: record[:detail],
        profile_image_url: record[:profile_image_url],
        url: record[:url],
        protected: record[:protected] != 0,
        followers_count: record[:followers_count],
        friends_count: record[:friends_count],
        statuses_count: record[:statuses_count],
        favourites_count: record[:favourites_count],
        created: record[:created].to_i } end

    User.add_data_retriever(new)
  end
end

Plugin.create(:sqlite) do
  db = Plugin::SQLite::DataSource.db
  ds_message = Plugin::SQLite::MessageDataSource.new

  on_favorite do |service, user, message|
    db[:favorites] << {
      user_id: user.id,
      message_id: message.id,
      created: Time.now.to_i }
  end

  filter_favorited_by do |message, users|
    result = db[:favorites].filter(message_id: message.id).map{ |r| r[:user_id] }.reject(&users.method(:include?))
    detect = User.findbyid(result, -2).select(&ret_nth)
    [message, users + detect] end

end
