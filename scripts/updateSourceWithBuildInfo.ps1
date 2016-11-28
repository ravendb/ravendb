
function UpdateCommonAssemblyInfo ( $projectDir ) {

    $commit = Get-Git-Commit

    if($global:buildlabel -ne $CUSTOM_BUILD_NUMBER) {
        $assemblyInfoFile = "$base_dir\CommonAssemblyInfo.cs"
        Write-Host "Modifying $assemblyInfoFile..."

        (Get-Content $assemblyInfoFile) |
        Foreach-Object { $_ -replace "{build-label}", "$($global:buildlabel)" } |
        Foreach-Object { $_ -replace "{commit}", $commit } |
        Foreach-Object { $_ -replace "{stable}", $global:uploadMode } |
        Foreach-Object { $_ -replace '\[assembly: AssemblyFileVersion\(".*"\)\]', "[assembly: AssemblyFileVersion(""$version.$global:buildlabel"")]" } |
        Foreach-Object { $_ -replace '\[assembly: AssemblyInformationalVersion\(".*"\)\]', "[assembly: AssemblyInformationalVersion(""$informationalVersion"")]" } |
        Set-Content $assemblyInfoFile -Encoding UTF8
    }
}
