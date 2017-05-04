$CUSTOM_SETTINGS_PATH = "c:\raven-config\$env:CustomConfigFilename"

cd c:/ravendb/Server

if ([string]::IsNullOrEmpty($env:CustomConfigFilename) -eq $False) {
    ./Raven.Server.exe `
        /Raven/RunAsService=true `
        /Raven/ServerUrl/Tcp=38888 `
        /Raven/Config="$CUSTOM_SETTINGS_PATH" `
        /Raven/DataDir=$($env:DataDir) `
        --print-id
}
else {
    ./Raven.Server.exe `
        /Raven/RunAsService=true `
        /Raven/ServerUrl/Tcp=38888 `
        /Raven/AllowEverybodyToAccessTheServerAsAdmin=$($env:AllowEverybodyToAccessTheServerAsAdmin) `
        /Raven/DataDir=$($env:DataDir) `
        --print-id
}
