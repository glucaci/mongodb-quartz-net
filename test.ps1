docker-compose -p mongo_quartz down 
docker-compose -p mongo_quartz rm

try {
    docker-compose -p mongo_quartz build
    docker-compose -p mongo_quartz up -d
    docker wait $(docker ps -aqf "name=myapp_mongo_quartz")
    # dotnet tool install -g dotnet-reportgenerator-globaltool
    # reportgenerator -reports:"tests/Quartz.Spi.MongoDbJobStore.Tests/coveragereport/coverage*.cobertura.xml" -targetdir:"coveragereport" -reporttypes:Html
}
finally {
    docker-compose -p mongo_quartz down 
    docker-compose -p mongo_quartz rm
}

