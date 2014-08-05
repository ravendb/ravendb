import commandBase = require("commands/commandBase");
import database = require("models/database");
import zip = require('jszip/jszip');
import zipUtils = require('jszip/jszip-utils.min');
import appUrl = require('common/appUrl');

class getInfoPackage extends commandBase {

    /**
	* @param ownerDb The database the collections will belong to.
	*/
    constructor(private db: database, private withStackTrace: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {
        var task = $.Deferred();
        var url = appUrl.forResourceQuery(this.db)  + (this.db.isSystem ? '/admin/debug/info-package' : '/debug/info-package');
        if (this.withStackTrace && this.db.isSystem) {
            url += "?stacktrace";
        }
        zipUtils.getBinaryContent(url, function (err, data) {
            if (err) {
                task.reject(err);
            } else {
                task.resolve(data);
            }            
        });

        return task.promise();
    }
}

export = getInfoPackage;