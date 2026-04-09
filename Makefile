IMAGE_NAME ?= steven-pyspark-import
IMAGE_TAG ?= $(shell date +%Y%m%d)

.DEFAULT_GOAL := build

.PHONY: build run

build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

run:
	docker run --rm -it --name $(IMAGE_NAME)-run --entrypoint bash $(IMAGE_NAME):$(IMAGE_TAG)
