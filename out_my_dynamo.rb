module Fluent
				class MyDynamo_Output < BufferedOutput
								Fluent::Plugin.register_output('mydynamo', self)
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
												aws = []
												open("/Users/tomoya/.aws_credential").each do |line|
																aws.push(line.split(" ").last)
												end

												AWS.config({
																:access_key_id => aws[0],
																:secret_access_key => aws[1],
																:dynamo_db_endpoint => "dynamodb.ap-northeast-1.amazonaws.com"
												})
												tracking = AWS::DynamoDB.new.tables['sometracking']
												tracking.hash_key = [:id, :string]
												tracking.range_key = [:date, :number]
												@items = tracking.items
								end

								def shutdown
								end

								def format(tag, time, record)
												record.to_msgpack
								end

								def write (chunk)
												chunk.msgpack_each do |record|
																if not record['id'].nil?
																				if @items[record['id'], record['date'].to_i].exists?
																								@items[record['id'], record['date'].to_i].attributes.update do |u|
																												u.add("value" => 1)
																								end
																				else
																								@items.create(:id => record['id'],
																																	:date => record['date'].to_i,
																																	'ad_id' => record['ad_id'],
																																	'pub_id' => record['pub_id'],
																																	'value' => 1)
																				end
																else
																				if @items[record['message'].split(':')[1], record['time'].split(':')[1].to_i].exists?
																								@items[record['message'].split(':')[1], record['time'].split(':')[1].to_i].attributess.update do |u|
																												u.add("value" => 1)
																								end
																				else
																								@items.create(:id => record['message'].split(':')[1],
																															:date => record['time'].split(':')[1].to_i,
																															'ad_id' => record['message'].split(':')[1],
																															'pub_id' => record['message'].split(':')[1],
																															'value' => 1)
																				end
																end
												end
								end
				end
end


