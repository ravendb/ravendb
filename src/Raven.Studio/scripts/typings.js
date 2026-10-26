
const path = require('path');

const exec = require('child_process').execSync;

const typingsGeneratorProject = path.resolve(__dirname, '../../../tools/TypingsGenerator/TypingsGenerator.csproj');

console.log("Generating typings...");

// Release, so the build does not touch the Debug output that the IDE uses to run the server.
// A running server locks its dlls, and a Debug build here would fail when it tries to replace them.
exec(`dotnet run -c Release --project "${typingsGeneratorProject}"`, {
    stdio: 'inherit'
});
