FROM ghcr.io/tvboelens-cr/kafka-lite-ci-base:latest AS build
COPY . /usr/src/kafka-lite
WORKDIR /usr/build/kafka-lite
RUN cmake -S /usr/src/kafka-lite -B . -DCMAKE_BUILD_TYPE=DEBUG \ 
    && make -j16 TestSuite

FROM ghcr.io/tvboelens-cr/kafka-lite-ci-base:latest
COPY --from=build /usr/build/kafka-lite/TestSuite /opt/kafka-lite/TestSuite
WORKDIR /opt/kafka-lite/
RUN ./TestSuite
