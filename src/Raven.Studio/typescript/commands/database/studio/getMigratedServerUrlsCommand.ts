import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getMigratedServerUrlsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Smuggler.Migration.MigratedServerUrls> {
        const url = endpoints.databases.smuggler.migrateGetMigratedServerUrls;
        return this.query(url, null, this.db);
    }
}

export = getMigratedServerUrlsCommand; 
