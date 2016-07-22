import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class renameIndexCommand extends commandBase {

    constructor(private existingIndexName: string, private newIndexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Renaming " + this.existingIndexName + "...");

        return this.rename()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to rename " + this.existingIndexName, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess("Renamed " + this.existingIndexName + " to " + this.newIndexName);
            });

    }

    private rename(): JQueryPromise<any> {
        var urlArgs = {
            newName: this.newIndexName
        };
        var url = "/indexes-rename/" + this.existingIndexName + this.urlEncodeArgs(urlArgs);
        return this.post(url, null, this.db, { dataType: undefined });
    }
}

export = renameIndexCommand;