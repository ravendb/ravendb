
const fsUtils = require("./fsUtils");

const exec = require('child_process').execSync;

const possibleTypingsGenPaths = [
    '../../tools/TypingsGenerator/bin/Debug/net9.0/TypingsGenerator.dll',
    '../../tools/TypingsGenerator/bin/Release/net9.0/TypingsGenerator.dll' ];

const dllPath = fsUtils.getLastRecentlyModifiedFile(possibleTypingsGenPaths);
if (!dllPath) {
    cb(new Error('TypingsGenerator.dll not found neither for Release nor Debug directory.'));
    return;
}

console.log("Generating typings...");

exec('dotnet ' + dllPath, {
    stdio: 'inherit',
    stderr: 'inherit'
});
