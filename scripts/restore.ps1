function DownloadDependencies () {
    dotnet restore
    CheckLastExitCode
}
