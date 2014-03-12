import commandBase = require("commands/commandBase");
import database = require("models/database");
import index = require("models/index");
import indexPriority = require("models/indexPriority");

class saveIndexDefinitionCommand extends commandBase {

    constructor(private index: indexDefinitionDto, private priority: indexPriority, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving " + this.index.Name + "...");

        // Saving an index definition requires 2 steps:
        // 1. PUT the index definition to / indexes / [index name]? definition = yes
        // 2. POST the index priority to / indexes / set - priority / [indexname]? priority = [priority]
        // 
        // These must be done in sequence because the index definition may be brand new, and thus setting the priority must wait for index creation.
        var result = $.Deferred();
        var runSavePriority = () => this.savePriority()
            .fail((response: JQueryXHR) => {
                this.reportWarning("Index was saved, but failed to set its priority.", response.responseText, response.statusText);
                result.reject(response);
            })
            .done(() => {
                this.reportSuccess("Saved " + this.index.Name + ".");
                result.resolve();
            });

        this.saveDefinition()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save " + this.index.Name, response.responseText, response.statusText);
                result.reject(response);
            })
            .done(() => runSavePriority());

        return result;
    }

    private savePriority(): JQueryPromise<any> {
        // Note: don't URL encode the priority. Doing so busts Raven's handler.
        var url = "/indexes/set-priority/" + this.index.Name + "?priority=" + index.priorityToString(this.priority);
        return this.post(url, null, this.db, { dataType: undefined });
    }

    private saveDefinition(): JQueryPromise<any> {
        var urlArgs = {
            definition: "yes"
        };
        var putArgs = JSON.stringify(this.index);
        var url = "/indexes/" + this.index.Name + this.urlEncodeArgs(urlArgs);
        return this.put(url, putArgs, this.db);
    }
}

export = saveIndexDefinitionCommand; 