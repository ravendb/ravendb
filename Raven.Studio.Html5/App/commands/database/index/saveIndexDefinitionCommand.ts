import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class saveIndexDefinitionCommand extends commandBase {

    constructor(private index: indexDefinitionDto, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving " + this.index.Name + "...");

        return this.saveDefinition()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save " + this.index.Name, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess("Saved " + this.index.Name + ".");
            });

    }

    private saveDefinition(): JQueryPromise<any> {
        var urlArgs = {
            definition: "yes"
        };
        var putArgs = JSON.stringify(this.index);
        var url = "/indexes/" + this.index.Name + this.urlEncodeArgs(urlArgs);
        return this.put(url, putArgs, this.db, null, 61 * 1000);
    }
}

export = saveIndexDefinitionCommand; 
