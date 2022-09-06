bash -c "/tools/reportgenerator -h"

bash -c "dotnet test /p:CollectCoverage=true /p:CoverletOutputFormat=cobertura -f net6.0"
bash -c "cp -r *.xml ./coveragereport"

bash -c "/tools/reportgenerator -reports:"**\coverage*.cobertura.xml" -targetdir:"coveragereport" -reporttypes:Html"
