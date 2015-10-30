$gitPath = "C:\Program Files\Git\bin\git.exe";
If (Test-Path $gitPath) {
} else {
    $gitPath = "C:\Program Files (x86)\Git\bin\git.exe";
}

$filterToInsert = 'expand --tabs=4 --initial'

&$gitPath config --global filter.raven-spacify.clean $filterToInsert

$filterThatWasInserted = &$gitPath config --global --get filter.raven-spacify.clean

if ($filterToInsert -eq $filterThatWasInserted) {
    Write-Host 'Git setup successful. Filter added.' -foregroundcolor "green"
} else {
    Write-Host 'Git setup failed. Filter was not added.' -foregroundcolor "red"
}
