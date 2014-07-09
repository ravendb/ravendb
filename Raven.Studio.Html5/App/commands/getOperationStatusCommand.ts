import commandBase = require("commands/commandBase");
import database = require("models/database");
import collection = require("models/collection");

class getOperationStatusCommand extends commandBase {

    /**
	* @param db The database the collections will belong to.
	*/
    constructor(private db: database, private operationId: number) {
        super();

        if (!this.db) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<collection[]> {
        var url = "/operation/status";

        var args = {
            id: this.operationId
        }

        return this.query(url, args, this.db);
/*        var urlParams = "?query=Tag%3A" + encodeURIComponent(this.collectionName) + "&allowStale=true";
        var deleteTask = this.query(url + urlParams, null, this.db);
        // deletion is made asynchronically so we infom user about operation start - not about actual completion. 
        deleteTask.done(() => this.reportSuccess("Scheduled deletion of " + this.displayCollectionName));
        deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete " + this.displayCollectionName, response.responseText, response.statusText));
        return deleteTask;*/
/*
        var args = {
            pageSize: 1024,
            getAdditionalData: true
        };

        var url = "/databases";

        var resultsSelector = (databases: databaseDto[]) => databases.map(db => new database(db.Name, db.Disabled));
        return this.query(url, args, null, resultsSelector);*/
    }
}

export = getOperationStatusCommand;