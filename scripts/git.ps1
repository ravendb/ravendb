function Get-Git-Commit
{
    if ((Get-File-Exists-On-Path "git.exe")){
        $gitLog = git log --oneline -1
        return $gitLog.Split(' ')[0]
    }
    else {
        return "0000000"
    }
}

function Get-Git-Commit-Full
{
    if ((Get-File-Exists-On-Path "git.exe")){
        $gitLog = git log -1 --format="%H"
        return $gitLog;
    }
    else {
        return "0000000000000000000000000000000000000000"
    }
}
