import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexFieldsCommand extends commandBase {

    constructor(private db: database, private map: string) {
        super();
    }

    execute(): JQueryPromise<string[]> {
        const url = endpoints.databases.studioIndex.studioIndexFields;
        const args = {
            Map: this.map
        };
        return this.post(url, JSON.stringify(args), this.db);
    }
} 

export = getIndexFieldsCommand;
