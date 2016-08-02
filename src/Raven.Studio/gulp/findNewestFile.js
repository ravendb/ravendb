var glob = require('glob');
var fs = require('fs');

module.exports = function findNewestFile(targetGlob) {
    return function (projectDir, srcFile, absSrcFile) {
        // find newest file based on *targetToScan* and return this and file to compare against
        var files = glob.sync(targetGlob);
        var newestFile = null;
        var newestFileTimestamp = null;

        files.forEach(function (file) {
            var stats = fs.statSync(file);
            var mtime = stats.mtime.getTime();

            if (newestFileTimestamp == null || mtime > newestFileTimestamp) {
                newestFileTimestamp = mtime;
                newestFile = file;
            }
        });

        return newestFile;
    }
}
