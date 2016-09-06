import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import transformer = require("models/database/index/transformer");

class saveTransformerLockModeCommand extends commandBase {

    /*
     * @param lockMode Should be either "Unlock", "LockedIngore", or "LockedError".
    */
    constructor(private transformerName: string, private lockMode: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving " + this.transformerName + "...");
        var args = {
            op: "lockModeChange",
            mode: this.lockMode
        };
        
        var url = "/transformers/" + this.transformerName + this.urlEncodeArgs(args);//TODO: use endpoints
        var saveTask = this.post(url, JSON.stringify(args), this.db, { dataType: 'text' });
        saveTask.done(() => this.reportSuccess("Saved " + this.transformerName));
        saveTask.fail((response: JQueryXHR) => this.reportError("Failed to save this " + this.transformerName, response.responseText));

        return saveTask;
    }
}

export = saveTransformerLockModeCommand; 
