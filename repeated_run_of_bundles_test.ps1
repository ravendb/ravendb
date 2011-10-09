$i = 1
while($true) {
  echo "try #$i"
  .\build\xunit.console.clr4.exe .\build\Raven.Bundles.Tests.dll
  cls
  $i = $i + 1
}