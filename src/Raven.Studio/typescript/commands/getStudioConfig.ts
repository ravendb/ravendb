import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import documentClass = require("models/database/documents/document");

class getStudioConfig extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<documentClass> {
        var url = "/studio-tasks/config";
        var getTask = this.query(url, null, this.db);
        return getTask;
    }
}

export = getStudioConfig;
