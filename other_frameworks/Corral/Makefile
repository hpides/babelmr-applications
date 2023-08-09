PROG_NAME = smile

.PHONY: all clean $(PROG_NAME) input_in_s3

.EXPORT_ALL_VARIABLES:
CORRAL_VERBOSE = true

all: $(PROG_NAME)

fetch:
	 aws s3 cp  --recursive --quiet "s3://smile-tcp/corral/" ./results/
	 cat results/*

build:
	go build -o $(PROG_NAME) .

test_run: 
	./$(PROG_NAME) --lambda --out "s3://smile-tcp/corral/" "s3://smile-tcp/Input/*"

clean:
	aws s3 rm "s3://smile-tcp/corral/" --recursive
	rm -rfv results/*