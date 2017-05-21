$TARGET_PLATFORM_SPECS = (
    @{
        "Name"      = "windows-x64";
        "Runtime"   = "win10-x64";
        "PkgType"   = "zip";
        "IsUnix"    = $False;
        "ShortName" = "windows";
    },
    @{
        "Name"      = "ubuntu.14.04-x64";
        "Runtime"   = "ubuntu.14.04-x64";
        "PkgType"   = "tar.bz2";
        "IsUnix"    = $True;
        "ShortName" = "ubuntu14";
    },
    @{
        "Name"      = "ubuntu.16.04-x64";
        "Runtime"   = "ubuntu.16.04-x64";
        "PkgType"   = "tar.bz2";
        "IsUnix"    = $True;
        "ShortName" = "ubuntu16";
    },
    @{
        "Name"      = "raspberry-pi";
        "Runtime"   = "";
        "PkgType"   = "tar.bz2";
        "IsUnix"    = $True;
        "ShortName" = "rpi"
    }
);

function GetTargetPlatforms( $TargetPlatform ) {

    $specs = $TARGET_PLATFORM_SPECS;

    if ([string]::IsNullOrEmpty($TargetPlatform) -eq $False) {
        $specs = $specs | Where-Object { $_.ShortName -eq $TargetPlatform.ToLowerInvariant() }
    }

    return $specs;

} 