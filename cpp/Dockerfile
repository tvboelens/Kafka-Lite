
FROM ubuntu:stonking AS build
COPY . /usr/src/kafka-lite
WORKDIR /usr/build/kafka-lite
RUN apt-get update \
    && apt-get install -y build-essential \
    && apt-get install -y cmake \
    && apt-get install libboost1.90-dev\
    && apt-get install -y libasio-dev \ 
    && cmake -S /usr/src/kafka-lite -B . -DCMAKE_BUILD_TYPE=DEBUG \
    && make -j16 TestSuite

FROM ubuntu:stonking
COPY --from=build /usr/build/kafka-lite/TestSuite /opt/kafka-lite/TestSuite
WORKDIR /opt/kafka-lite/
RUN apt-get update \
    && apt-get install -y libboost1.90-dev \ 
    && apt-get install -y libasio-dev
ENTRYPOINT ["/opt/kafka-lite/TestSuite"]
