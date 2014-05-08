$test_prefix = $args[0]
$base_dir  = resolve-path .
$sln_file = "$base_dir\RavenDB.sln"
$global:configuration = "Debug"
$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name

$xunit = "$base_dir\Raven.Xunit.Runner\bin\$global:configuration\Raven.Xunit.Runner.exe"
$test_projects = @( `
		"$base_dir\Raven.Tests.Core\bin\$global:configuration\Raven.Tests.Core.dll", `
		"$base_dir\Raven.Tests\bin\$global:configuration\Raven.Tests.dll", `
		"$base_dir\Raven.Tests.Bundles\bin\$global:configuration\Raven.Tests.Bundles.dll", `
		"$base_dir\Raven.Tests.Issues\bin\$global:configuration\Raven.Tests.Issues.dll", `
		"$base_dir\Raven.Tests.MailingList\bin\$global:configuration\Raven.Tests.MailingList.dll", `
		"$base_dir\Raven.SlowTests\bin\$global:configuration\Raven.SlowTests.dll", `
		"$base_dir\Raven.DtcTests\bin\$global:configuration\Raven.DtcTests.dll", `
		"$base_dir\Raven.Voron\Voron.Tests\bin\$global:configuration\Voron.Tests.dll", `
		"$base_dir\RavenFS.Tests\bin\$global:configuration\RavenFS.Tests.dll")

&"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$sln_file" /p:Configuration=$global:configuration /p:nowarn="1591 1573" /verbosity:quiet /nologo /maxcpucount /consoleloggerparameters:ErrorsOnly
if($LastExitCode -ne 0) {
	exit 125
}

foreach ($test_project in $test_projects) {
	&"$xunit" $test_project $test_prefix
	if($LastExitCode -ne 0) {
		exit $LastExitCode
	}
}

exit 0