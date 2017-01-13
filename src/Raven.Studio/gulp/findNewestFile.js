var glob = require('glob');
var fsUtils = require('./fsUtils');

module.exports = function findNewestFile(targetGlob) {
    return function (projectDir, srcFile, absSrcFile) {
        // find newest file based on *targetToScan* and return this and file to compare against
        var files = glob.sync(targetGlob);
        return fsUtils.getLastRecentlyModifiedFile(files);    
    }
}
