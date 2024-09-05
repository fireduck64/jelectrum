# Not a complete docker file
# just sets up dependancies and such and does the compile
# needs to actually run the thing and manage configs and such
# Anyways....

FROM debian:stable AS build
  
ARG DEBIAN_FRONTEND=noninteractive
RUN mkdir -p /usr/share/man/man1
RUN apt-get update
RUN apt-get install -y gnupg git default-jdk-headless curl
RUN echo "deb [arch=amd64] http://storage.googleapis.com/bazel-apt testing jdk1.8" > /etc/apt/sources.list.d/bazel.list
RUN curl https://bazel.build/bazel-release.pub.gpg | apt-key add -
RUN apt-get update
RUN apt-get install -y bazel

COPY .git /jelectrum/.git

WORKDIR /jelectrum
RUN git checkout .
RUN bazel --version
RUN bazel build :all :Jelectrum_deploy.jar




