
const path = require('path');
const fs = require('fs');

console.log("Patching Durandal...");

const sourceDurandalFile = path.resolve(__dirname, "../wwwroot/Content/custom_durandal_system.js");
const targetDurandalFile = path.resolve(__dirname, "../node_modules/durandal/js/system.js");

fs.copyFileSync(sourceDurandalFile, targetDurandalFile);

console.log("Patching JSZip");

const sourceJsZipFile = path.resolve(__dirname, "../typings/custom_jszip.d.ts");
const targetJsZipFile = path.resolve(__dirname, "../node_modules/jszip/index.d.ts");

fs.copyFileSync(sourceJsZipFile, targetJsZipFile);


// to make builds easier make sure wwwroot/lib, typings/globals, typings/modules doesn't exist - to prevent compilation errors

const dirs = [
    "wwwroot/lib",
    "typings/globals",
    "typings/modules"
]

for (const dir of dirs) {
    if (fs.existsSync(dir)) {
        console.log("Removing " + dir);
        fs.rmSync(dir, { recursive: true, force: true });
    }
}
