import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import { shardingTodo } from "common/developmentHelper";

class getIndexNamesCommand extends commandBase {

    private db: database;

    private readonly location: databaseLocationSpecifier = null;

    constructor(db: database, location: databaseLocationSpecifier = null) {
        super();
        this.location = location;
        this.db = db;
        shardingTodo("Danielle"); // TODO - location param should not be optional
    }

    execute(): JQueryPromise<string[]> {
        const args = {
            namesOnly: true,
            pageSize: 1024,
            ...this.location
        };
        
        const url = endpoints.databases.index.indexes;
        return this.query(url, args, this.db, (x: resultsDto<string>) => x.Results)
            .fail((response: JQueryXHR) => this.reportError("Failed to get the database indexes", response.responseText, response.statusText));
    }
} 

export = getIndexNamesCommand;
