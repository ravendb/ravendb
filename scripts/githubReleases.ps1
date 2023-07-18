function CreateRelease($version, $owner, $repo, $branch, $token, $changelog, $dryRun)
{
    $obj = @{
        body = "$changelog"
        tag_name = "$version"
        target_commitish = "$branch"
        name = "$version"
        draft = $false
        prerelease = $false
        generate_release_notes = $false
    }
    
    $json = $obj | ConvertTo-Json
    
    $headers = @{
        "Accept" = "application/vnd.github+json"
        "Authorization" = "Bearer $token"
        "X-GitHub-Api-Version" = "2022-11-28"
    }

    if($dryRun){
        Write-Host "DRY RUN: Created release in $owner/$repo ${branch}:"
        Write-Host "DRY RUN: $json"
        return;
    }
    
    Invoke-WebRequest -Uri https://api.github.com/repos/$owner/$repo/releases -Method POST -Body $json -Headers $headers
}