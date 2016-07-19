# put your server URL here ...
$serverUrl = "http://localhost:8080";

# ... and ApiKey
$apiKey = "key1/sAdVA0KLqigQu67Dxj7a";



function GetApiKeyFromServer($serverUrl, $apiKey) {
    $psi = New-object System.Diagnostics.ProcessStartInfo 
    $psi.CreateNoWindow = $true 
    $psi.UseShellExecute = $false 
    $psi.RedirectStandardOutput = $true 
    $psi.RedirectStandardError = $true 
    $psi.FileName = (Get-Item -Path ".\Raven.ApiToken.exe" -Verbose).FullName
    $psi.Arguments = "$serverUrl $apiKey"
    $process = New-Object System.Diagnostics.Process 
    $process.StartInfo = $psi
    [void]$process.Start()
    $output = $process.StandardOutput.ReadToEnd() 
    $process.WaitForExit() 
    if ($process.ExitCode -ne 0) {
        Write-Error $process.StandardError.ReadToEnd()
        Exit;
    }
    $output
}

$TOKEN = GetApiKeyFromServer($serverUrl, $apiKey)
$header = @{"Authorization"="Bearer $TOKEN"}
Invoke-RestMethod -Uri "http://localhost:8080/stats" -Method Get -Headers $header

