
function Update-NuGet
{
	$packageName = "Newtonsoft.Json";
	Remove-Item */*.g.3.5.csproj
	Remove-Item */*.g.csproj
	(Get-ChildItem */packages.config) + (Get-ChildItem */*/packages.config) | 
		ForEach-Object { .\Tools\NuGet.exe update $_ -Id "$packageName" -RepositoryPath packages -Verbose }
}