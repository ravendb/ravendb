.\psake.ps1 default.ps1 -task Upload

if($lastexitcode -ne 0)
{
	throw 'Could not build OSS Version successfully'
}

.\psake.ps1 default.ps1 -task UploadCommercial

if($lastexitcode -ne 0)
{
	throw 'Could not build Commercial Version successfully'
}
