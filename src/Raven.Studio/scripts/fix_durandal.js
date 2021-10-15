
var path = require('path');

var sourceFile = path.resolve("wwwroot/Content/custom_durandal_system.js");
var targetFile = path.resolve("node_modules/durandal/js/system.js");

console.log("Patching Durandal...");

const fs = require('fs');
fs.copyFileSync(sourceFile, targetFile);
