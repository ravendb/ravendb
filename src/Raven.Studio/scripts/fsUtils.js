const fs = require('fs');

module.exports = {
    getLastRecentlyModifiedFile(paths) {
        var result = paths.map(x => {
            var stat = null;
            try {
                stat = fs.statSync(x);
            } catch (err) {
                if (err.code !== 'ENOENT') {
                    throw err;
                }
            }
            return {
                filePath: x,
                stat
            };
        }).reduce((result, item) => {
            if (item.stat === null) {
                return result;
            }

            if (result && result.stat && result.stat.mtime.getTime() > item.stat.mtime.getTime()) {
                return result;
            }

            return item;
        }, null);

        return result ? result.filePath : null;
    }
}
