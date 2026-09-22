IMAGE_NAME ?= steven-pyspark-import
IMAGE_TAG ?= 20260410-base

IMAGE_TAR ?= $(IMAGE_NAME)-$(IMAGE_TAG).tar.gz

.DEFAULT_GOAL := build

.PHONY: build run save

build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

run:
	docker run --rm -it --name $(IMAGE_NAME)-run --entrypoint bash $(IMAGE_NAME):$(IMAGE_TAG)

save:
	docker save $(IMAGE_NAME):$(IMAGE_TAG) | gzip > $(IMAGE_TAR)
