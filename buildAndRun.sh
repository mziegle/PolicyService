#!/usr/bin/env bash

docker build --rm -t policy_service:build .

docker create --name policy_service_build policy_service:build

rm -rf run/target

docker cp policy_service_build:/build/target run

docker build --rm -t mziegle1/policy_service:run run

docker rm -f policy_service_build

# Remove intermediate containers which have appeared during the build process
docker rmi $(docker images -f "dangling=true" -q)

docker run \
    -p 50032:50032 \
    -e DB_SERVER=docker.for.mac.localhost \
    -e DB_USER=user \
    -e DB_PASSWORD=user \
    -e DB_NAME=insurance_policy \
    -e POLICY_SERVICE_PORT=50032 \
    -e CUSTOMER_SERVICE_PORT=50031 \
    -e CUSTOMER_SERVICE_HOST=docker.for.mac.localhost \
    -i -t --security-opt=seccomp:unconfined \
    --rm mziegle1/policy_service:run