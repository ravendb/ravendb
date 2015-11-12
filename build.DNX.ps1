$version = "1.0.0-rc1-16048"
$architecture = "x64"
$toolsDir = "Tools\DNX"
$dnvm = "$toolsDir\dnvm.cmd"
$runtimeDir = "$env:USERPROFILE\.dnx\runtimes\dnx-coreclr-win-$architecture.$version\bin";
$dnu = "$runtimeDir\dnu.cmd"
$dnx = "$runtimeDir\dnx.exe"

&"$dnvm" install $version -u -r coreclr -arch $architecture

&"$dnvm" use $version

&"$dnu" restore --quiet --parallel Raven.Sparrow\Sparrow Raven.Abstractions Raven.Client.Lightweight Raven.Tests.Core

Push-Location "Raven.Tests.Core"

&"$dnx" test

Pop-Location