var fileExists = require('file-exists');

module.exports = function checkAllFilesExist(files) {
    files.forEach(function(e) {
        if (!fileExists(e)) {
            throw new Error("Unable to find file: " + e);
        }
    });
}