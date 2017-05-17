$CUSTOM_SETTINGS_PATH = "c:\raven-config\$env:CustomConfigFilename"

cd c:/ravendb/Server

if ([string]::IsNullOrEmpty($env:CustomConfigFilename) -eq $False) {
    ./Raven.Server.exe `
        /Raven/RunAsService=true `
        /Raven/ServerUrl=http://0.0.0.0:8080 `
        /Raven/ServerUrl/Tcp=tcp://0.0.0.0:38888 `
        /Raven/Config="$CUSTOM_SETTINGS_PATH" `
        /Raven/DataDir=$($env:DataDir) `
        --print-id
}
else {
    ./Raven.Server.exe `
        /Raven/RunAsService=true `
        /Raven/ServerUrl=http://0.0.0.0:8080 `
        /Raven/ServerUrl/Tcp=tcp://0.0.0.0:38888 `
        /Raven/AllowAnonymousUserToAccessTheServer=$($env:AllowAnonymousUserToAccessTheServer) `
        /Raven/DataDir=$($env:DataDir) `
        --print-id
}
