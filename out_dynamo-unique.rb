module Fluent
		class Dynamo_OutputUnique < BufferedOutput
				Fluent::Plugin.register_output('dynamo-unique', self)
				attr_reader :host, :port, :kpi_items

				def initialize
						super
						require "aws-sdk"
						require "msgpack"
				end

				def counfigure (conf)
						@host = couf.has_key?('host') ? conf['host'] : 'localhost'
						@port = conf.has_key?('port') ? conf['port'].to_i : 6379
				end

				def start
						super
						AWS.config({
								:access_key_id => '**********',
								:secret_access_key => '********',
								:dynamo_db_endpoint => "*********"
						})
						kpi_table = AWS::DynamoDB.new.tables['kpi_table']
						kpi_table.hash_key = [:id, :string]
						kpi_table.range_key = [:date, :number]
						@kpi_items = kpi_table.items
				end

				def shutdown
				end

				def format(tag, time, record)
						record.to_msgpack
				end

				def write (chunk)
						chunk.msgpack_each do |record|
										if not record['id'].nil?
												unless @kpi_items[record['id'], record['date'].to_i].exists?
												@kpi_items.create(:id => record['id'],
																  :date => record['date'].to_i,
																  'ad_id' => record['ad_id'],
																  'pub_id' => record['pub_id'],
																  'value' => 1)
												end
										end
						end
				end
		end
end


