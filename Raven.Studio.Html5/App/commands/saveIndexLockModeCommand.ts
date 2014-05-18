import commandBase = require("commands/commandBase");
import database = require("models/database");
import index = require("models/index");

class saveIndexLockModeCommand extends commandBase {

    /*
     * @param lockMode Should be either "Unlock", "LockedIngore", or "LockedError".
    */
    constructor(private index: index, private lockMode: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving " + this.index.name + "...");
        var args = {
            op: "lockModeChange",
            mode: this.lockMode
        };
        
        // Deliberately not encoding index name in URI. Doing this breaks Raven's URL handling, resulting in a 400 bad request.
        var url = "/indexes/" + this.index.name + this.urlEncodeArgs(args);
        var saveTask = this.post(url, JSON.stringify(args), this.db, { dataType: 'text' });
        saveTask.done(() => {
             debugger;
             this.reportSuccess("Saved " + this.index.name);
        });
        saveTask.fail((response: JQueryXHR) => {
            debugger;
            this.reportError("Failed to save this" + this.index.name, response.responseText);

        });

        return saveTask;
    }
} 

export = saveIndexLockModeCommand;