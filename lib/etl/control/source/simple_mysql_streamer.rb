require 'mysql2'

class SimpleMysqlStreamer
  
  def initialize(query, target, connection)
    # Lets just be safe and also make sure there aren't new lines
    # in the SQL - its bound to cause trouble
    @query = query.split.join(' ')
    @name = target
    @first_row = connection.select_all("#{query} LIMIT 1")
    @config = ETL::Base.configurations[@name.to_s]
  end

  def any?
    @first_row.any?
  end 

  def first
    @first_row.first
  end

  def each
    Mysql2::Client.new(@config).query(@query, stream: true).each do |result|
      yield result
    end
  end

end