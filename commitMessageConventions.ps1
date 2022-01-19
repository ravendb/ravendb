
$allCommits = $($env:commits) | ConvertFrom-Json
$allMatched = $TRUE

Foreach ($commit in $allCommits)
{
    $message = $commit.commit.message
    Write-Host "Processing message '$message'"

	$loweredMessage = $message.ToLowerInvariant()
    $match = $loweredMessage -match "ravendb-\d+" -or $loweredMessage -match "rdoc-\d+" -or $loweredMessage -match "rdbqa-\d+" -or $loweredMessage -match "rdbc-\d+" -or $loweredMessage -match "rdbs-\d+" -or $loweredMessage -match "rdbcl-\d+" -or $loweredMessage -match "merge branch" -or $loweredMessage -match "merge remote" -or $loweredMessage -match "merge pull request"

	if ($match -eq $FALSE)
	{
		$allMatched = $FALSE
		Write-Host "Commit message '$message' does not contain issue #"
	}
}

if ($allMatched -eq $FALSE)
{
	throw "Not all commit messages contain issue #"
}