import commandBase = require("commands/commandBase");

class createDatabaseCommand extends commandBase {

    constructor(private databaseName: string) {
        super();
    }

    execute(): JQueryPromise<any> {
        var createDbTask = this.ravenDb.createDatabase(this.databaseName);

        this.reportInfo("Creating " + this.databaseName);

        createDbTask.done(() => this.reportSuccess(this.databaseName + " created"));
        createDbTask.fail((response) => this.reportError("Failed to create database", JSON.stringify(response)));
        return createDbTask;
    }
}

export = createDatabaseCommand;