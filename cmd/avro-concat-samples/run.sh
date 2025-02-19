#!/bin/sh

input1=./sample.d/input1.avro
input2=./sample.d/input2.avro
input3=./sample.d/input3.avro

geninput(){
	echo generating input...

	export ENV_SCHEMA_FILENAME=./sample.d/input.avsc

	printf \
		'%s\n' \
		'{"name":"fuji",  "value":3.776}' \
		'{"name":"takao", "value":0.599}' |
		json2avrows |
		cat > "${input1}"

	printf \
		'%s\n' \
		'{"name":"sky",   "value":0.333}' \
		'{"name":"tokyo", "value":0.634}' |
		json2avrows |
		cat > "${input2}"

	printf \
		'%s\n' \
		'{"name":"run",  "value":42.195}' \
		'{"name":"unit", "value": 0.001}' |
		json2avrows |
		cat > "${input3}"

}

test -f "${input3}" || geninput

export ENV_NUM_OF_SAMPLES_PER_FILE=1
export ENV_SCHEMA_FILENAME=./sample.d/input.avsc

find ./sample.d -type f -name '*.avro' |
	sort |
	./avro-concat-samples |
	rq -aJ |
	jq -c
