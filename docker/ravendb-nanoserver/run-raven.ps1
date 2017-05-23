$CUSTOM_SETTINGS_PATH = "c:\raven-config\$env:CustomConfigFilename"

cd c:/ravendb/Server

if ([string]::IsNullOrEmpty($env:CustomConfigFilename) -eq $False) {
    ./Raven.Server.exe `
        /Raven/ServerUrl=http://0.0.0.0:8080 `
        /Raven/ServerUrl/Tcp=tcp://0.0.0.0:38888 `
        /Raven/DataDir=$($env:DataDir) `
        --config-path "$CUSTOM_SETTINGS_PATH" `
        --run-as-service `
        --print-id
}
else {
    ./Raven.Server.exe `
        /Raven/ServerUrl=http://0.0.0.0:8080 `
        /Raven/ServerUrl/Tcp=tcp://0.0.0.0:38888 `
        /Raven/AllowAnonymousUserToAccessTheServer=$($env:AllowAnonymousUserToAccessTheServer) `
        /Raven/DataDir=$($env:DataDir) `
        --run-as-service `
        --print-id
}
