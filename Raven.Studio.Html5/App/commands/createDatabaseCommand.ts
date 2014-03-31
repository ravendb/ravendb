import commandBase = require("commands/commandBase");

class createDatabaseCommand extends commandBase {

    constructor(private databaseName: string, private settings: {}, private securedSettings: {}) {
        super();

        if (!databaseName) {
            this.reportError("Database must have a name!");
            throw new Error("Database must have a name!");
        }
    }

    execute(): JQueryPromise<any> {

        this.reportInfo("Creating " + this.databaseName);
        var databaseDoc = {
            "Settings": this.settings,
            "SecuredSettings": this.securedSettings, // TODO: based on the selected bundles, we may need to include additional settings here
            "Disabled": false
        };

        var url = "/admin/databases/" + this.databaseName;
        var createTask = this.put(url, JSON.stringify(databaseDoc), null, { dataType: undefined });
        createTask.done(() => this.reportSuccess(this.databaseName + " created"));
        createTask.fail((response: JQueryXHR) => this.reportError("Failed to create database", response.responseText, response.statusText));

        // Forces creation of standard indexes? Looks like it.
        createTask.done(() => this.query("/databases/" + this.databaseName + "/silverlight/ensureStartup", null, null));

        return createTask;
    }
}

export = createDatabaseCommand; 