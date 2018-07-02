#tool nuget:?package=NUnit.ConsoleRunner&version=3.4.0
//////////////////////////////////////////////////////////////////////
// ARGUMENTS
//////////////////////////////////////////////////////////////////////

var target = Argument("target", "Default");
var configuration = Argument("configuration", "Release");

//////////////////////////////////////////////////////////////////////
// PREPARATION
//////////////////////////////////////////////////////////////////////

Teardown(context =>
{
    // Appveyor is failing to exit the cake script.
    if (AppVeyor.IsRunningOnAppVeyor)
    {
        foreach (var process in Process.GetProcessesByName("dotnet"))
        {
            process.Kill();
        }
    }
});

//////////////////////////////////////////////////////////////////////
// TASKS
//////////////////////////////////////////////////////////////////////

Task("Clean")
    .Does(() =>
{
    CleanDirectory("./artifacts/");
});

Task("Restore")
    .IsDependentOn("Clean")
    .Does(() =>
{
    DotNetCoreRestore();
});

Task("Build")
    .IsDependentOn("Restore")
    .Does(() =>
{
    var settings = new DotNetCoreMSBuildSettings();
    settings.SetConfiguration(configuration);
    DotNetCoreMSBuild(settings);
});

Task("Test")
    .IsDependentOn("Build")
    .Does(() =>
{
    DotNetCoreTest("./tests/Quartz.Spi.MongoDbJobStore.Tests/Quartz.Spi.MongoDbJobStore.Tests.csproj");
});

Task("Pack")
    .IsDependentOn("Build")
    .Does(() => 
{
    var settings = new DotNetCorePackSettings
    {
        Configuration = configuration,
        OutputDirectory = "./artifacts/"
    };

    DotNetCorePack("./src/Quartz.Spi.MongoDbJobStore/Quartz.Spi.MongoDbJobStore.csproj", settings);
});

Task("AppVeyor")
    .IsDependentOn("Test")
    .IsDependentOn("Pack")
    .Does(() => 
{

});

//////////////////////////////////////////////////////////////////////
// TASK TARGETS
//////////////////////////////////////////////////////////////////////

Task("Default")
    .IsDependentOn("Build");

//////////////////////////////////////////////////////////////////////
// EXECUTION
//////////////////////////////////////////////////////////////////////

RunTarget(target);
