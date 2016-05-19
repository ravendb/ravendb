import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import index = require("models/database/index/index");
import indexPriority = require("models/database/index/indexPriority");

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
            definition: "yes",
            name: this.index.Name
        };
        var putArgs = JSON.stringify(this.index);
        var url = "/indexes" + this.urlEncodeArgs(urlArgs);
        return this.put(url, putArgs, this.db);
    }
}

export = saveIndexDefinitionCommand; 
