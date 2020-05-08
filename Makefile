.PHONY : all package docker_build docker_push test

# Controls the version of the jar (and the version of the docker image with that jar)
PROJECT_VERSION ?= 1.0.0-SNAPSHOT

all: build run

package:
	mvn package -Dproject_version=$(PROJECT_VERSION) -DskipTests

docker_build: package
	docker build . --build-arg PROJECT_VERSION=$(PROJECT_VERSION) -t spothero/kafka-connect-fs:$(PROJECT_VERSION)

docker_push: docker_build
	docker push spothero/kafka-connect-fs:$(PROJECT_VERSION)

test:
	mvn test -Dproject_version=$(PROJECT_VERSION)
