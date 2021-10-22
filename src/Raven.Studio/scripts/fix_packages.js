
const path = require('path');
const fs = require('fs');

const sourceDurandalFile = path.resolve("wwwroot/Content/custom_durandal_system.js");
const targetDurandalFile = path.resolve("node_modules/durandal/js/system.js");

console.log("Patching Durandal...");

fs.copyFileSync(sourceDurandalFile, targetDurandalFile);

console.log("Patching JSZip");

const sourceJsZipFile = path.resolve("typings/custom_jszip.d.ts");
const targetJsZipFile = path.resolve("node_modules/jszip/index.d.ts");

fs.copyFileSync(sourceJsZipFile, targetJsZipFile);
