import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

type rawDisableDatabaseResult = {
    Name: string;
    Success: boolean;
    Reason: string;
    Disabled: boolean;
}

class disableDatabaseToggleCommand extends commandBase {

    constructor(private dbs: Array<database>, private disable: boolean) {
        super();
    }

    get action() {
        return this.disable ? "disable" : "enable";
    }

    execute(): JQueryPromise<Array<disableDatabaseResult>> {
        const args = {
            name: this.dbs.map(x => x.name)
        };

        const endPoint = this.disable ?
            endpoints.admin.adminResources.disable :
            endpoints.admin.adminResources.enable;


        //TODO: use static endpoint

        const url = "/admin/" + this.dbs[0].urlPrefix + endPoint + this.urlEncodeArgs(args);

        const task = $.Deferred<Array<disableDatabaseResult>>();

        this.post(url, null)
            .done(result => task.resolve(this.extractAndMapResult("db", result)))
            .fail((response: JQueryXHR) => this.reportError("Failed to toggle database status", response.responseText, response.statusText));

        return task;
    }

    private extractAndMapResult(qualifer: string, result: Array<rawDisableDatabaseResult>): Array<disableDatabaseResult> {
        return result.map(x => ({
            QualifiedName: qualifer + "/" + x.Name,
            Success: x.Success,
            Reason: x.Reason,
            Disabled: x.Disabled
        }));
    }

}

export = disableDatabaseToggleCommand;  
