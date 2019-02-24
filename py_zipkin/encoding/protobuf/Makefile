update-protobuf:
	curl -L https://raw.githubusercontent.com/openzipkin/zipkin-api/master/zipkin.proto > zipkin.proto
	protoc --python_out=. zipkin.proto
