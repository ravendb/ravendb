function SetSchemaInfoInTeamCity($projectDir) {
    $schemaVersionFile = Join-Path $projectDir -ChildPath "src\Raven.Server\Storage\Schema\SchemaUpgrader.cs"
    $currentVersionClassRegex = [regex]'(?sm)internal class CurrentVersion[\s\r\n]*{[^}]*'
    $content = Get-Content -Raw $schemaVersionFile
    $m = [regex]::Match($content, $currentVersionClassRegex)
    $versions = $m[0]

    $versionRegex = [regex]'(?sm)public const int ([A-Za-z]+Version) = (\d+);'
    $versions `
        | Select-String -Pattern $versionRegex -AllMatches `
        | ForEach-Object { $_.Matches } `
        | ForEach-Object { SetTeamCityEnvironmentVariable "RavenDB_Schema_$($_.Groups[1].Value)" $_.Groups[2].Value }
}

