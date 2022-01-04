FROM golang:1.17.5-bullseye as build
RUN go env -w GOPROXY=direct
# cache dependencies
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
# build
COPY *.go ./
RUN go build -o main
# copy artifacts to a clean image
FROM debian:bullseye-slim
ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie /usr/bin/aws-lambda-rie
RUN chmod 755 /usr/bin/aws-lambda-rie
COPY entry.sh /
COPY --from=build /src/main /main
ENTRYPOINT [ "/entry.sh" ]
