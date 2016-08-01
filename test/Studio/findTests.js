/*jslint node: true */
'use strict';

var fs = require('fs');

var path = [];

function endsWith(str, suffix) {
    return str.indexOf(suffix, str.length - suffix.length) !== -1;
}

function listFiles(path, extension) {
    console.log('scanning folder: ' + __dirname + '/' + path);

    var dirs = [__dirname + '/' + path];
    var files = [];

    var currentPath;

    function iterate(item, index) {
        var filename = currentPath + '/' + item;

        var stat = fs.statSync(filename);
        if (stat.isDirectory()) {
            // console.log('found directory: ' + filename);
            dirs.push(filename);
            return;
        }
        if (endsWith(item, extension)) {
            // console.log('adding file: ' + filename);
            files.push(filename);
            return;
        }
        // console.log('ignoring resource: ' + filename);
    }

    while(dirs.length) {
        var dir = dirs.shift();
        currentPath = dir;

        var list = fs.readdirSync(dir);
        list.forEach(iterate);
    }

    return files;
}

var files = listFiles('js', '-test.js');
// console.log('found test files:', files);

var text = '// auto generated unit tests lists based on file extension\n' +
    '// do not commit this file!\n' +
    'var tests = [\n';

console.log('------------');
files.forEach(function(item, index) {
    var file = item.substring(__dirname.length + 1);
    console.log('found test: ' + file);
    text += '    \'' + file + '\',\n';
});
console.log(' ');
// remove trailing comma
if (endsWith(text, ',')) text = text.substring(0, text.length - 1);

text += '];';

console.log('writing setup/tests.js ...');
fs.writeFile(__dirname + '/setup/tests.js', text);