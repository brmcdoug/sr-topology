#REGISTRY_NAME?=us-west1-docker.pkg.dev/fourth-compound-189519/jalapeno
REGISTRY_NAME?=docker.io/iejalapeno
IMAGE_VERSION?=latest
.PHONY: all sr-topology container push clean test

ifdef V
TESTARGS = -v -args -alsologtostderr -v 5
else
TESTARGS =
endif

all: sr-topology

sr-topology:
	mkdir -p bin
	$(MAKE) -C ./cmd compile-sr-topology

sr-topology-container: sr-topology
	docker build -t $(REGISTRY_NAME)/sr-topology:$(IMAGE_VERSION) -f ./build/Dockerfile.sr-topology .

push: sr-topology-container
	docker push $(REGISTRY_NAME)/sr-topology:$(IMAGE_VERSION)

clean:
	rm -rf bin

test:
	GO111MODULE=on go test `go list ./... | grep -v 'vendor'` $(TESTARGS)
	GO111MODULE=on go vet `go list ./... | grep -v vendor`
