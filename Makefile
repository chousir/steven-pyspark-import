IMAGE := steven-pyspark-import
TAG := $(shell date +%Y%m%d)-bgp
BASE_IMAGE := $(IMAGE):20260410-base
CONTAINER_COMMAND := docker build \
	--build-arg BASE_IMAGE=$(BASE_IMAGE) \
	-t $(IMAGE):$(TAG) .

all: build

build:
	$(CONTAINER_COMMAND)
