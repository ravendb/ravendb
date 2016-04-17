import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import index = require("models/database/index/index");
import indexPriority = require("models/database/index/indexPriority");

class saveIndexPriorityCommand extends commandBase {

    constructor(private indexName: string, private priority: indexPriority, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving " + this.indexName + " priority ...");
        // Note: don't URL encode the priority. Doing so busts Raven's handler.
        var url = "/indexes-set-priority/"  + this.indexName + "?priority=" + index.priorityToString(this.priority);
        return this.post(url, null, this.db, { dataType: undefined })
            .done(() => {
                this.reportSuccess("Saved " + this.indexName + ".");
            });
    }
}

export = saveIndexPriorityCommand; 
