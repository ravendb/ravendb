function ParseEnvSwitch ($value, $default = $False) {
    if ([string]::IsNullOrEmpty($value)) { 
        return $default
    } 
        
    return ![bool]::Parse($value)
}

function InitGlobals($debugSwitch, $noBundlingSwitch) {

    if ($noBundlingSwitch.IsPresent) {
        $global:isPublishBundlingEnabled = $false
    } else {
        $global:isPublishBundlingEnabled = !(ParseEnvSwitch $env:PUBLISH_NO_BUNDLING)
    }

    if ($debugSwitch.IsPresent) {
        $global:isPublishConfigurationDebug = $true
    } else {
        $global:isPublishConfigurationDebug = ParseEnvSwitch $env:PUBLISH_CONFIGURATION_DEBUG
    }

    Write-Host "Bundling: $global:isPublishBundlingEnabled"
    Write-Host "Debug: $global:isPublishConfigurationDebug"
}