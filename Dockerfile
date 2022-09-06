FROM mcr.microsoft.com/dotnet/sdk:6.0 as build
ENV USE_DOCKER_ENV=1
COPY . /mysrc

RUN dotnet tool install --tool-path /tools dotnet-reportgenerator-globaltool

WORKDIR /mysrc/tests/Quartz.Spi.MongoDbJobStore.Tests

VOLUME [ "/mysrc/tests/Quartz.Spi.MongoDbJobStore.Tests/coveragereport" ]

CMD [ "bash", "-c","./test.sh" ]