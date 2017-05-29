$TARGET_SPECS = (
    @{
        "Name"      = "windows-x64";
        "Runtime"   = "win10-x64";
        "PkgType"   = "zip";
        "IsUnix"    = $False;
        "TargetId" = "win-x64";
    },
    @{
        "Name"      = "windows-x86";
        "Runtime"   = "win10-x86";
        "Arch"      = "x86";
        "PkgType"   = "zip";
        "IsUnix"    = $False;
        "TargetId" = "win-x86";
    },
    @{
        "Name"      = "ubuntu.14.04-x64";
        "Runtime"   = "ubuntu.14.04-x64";
        "PkgType"   = "tar.bz2";
        "IsUnix"    = $True;
        "TargetId" = "ubuntu14";
    },
    @{
        "Name"      = "ubuntu.16.04-x64";
        "Runtime"   = "ubuntu.16.04-x64";
        "PkgType"   = "tar.bz2";
        "IsUnix"    = $True;
        "TargetId" = "ubuntu16";
    },
    @{
        "Name"      = "raspberry-pi";
        "Runtime"   = "";
        "PkgType"   = "tar.bz2";
        "IsUnix"    = $True;
        "TargetId" = "rpi"
    }
);

function GetBuildTargets( $targets ) {

    if (($targets -eq $null) -or ($targets.Count -eq 0)) {
        return $TARGET_SPECS;
    }

    $result = @( );

    foreach ($spec in $TARGET_SPECS) {
        foreach ($target in $targets) {
            if ($spec.TargetId -eq $target.ToLowerInvariant()) {
                $result += $spec
            }
        }
    }

    return $result;
} 