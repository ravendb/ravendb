import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getDatabasesCommand extends commandBase {
    
    execute(): JQueryPromise<database[]> {
        var args = {
            pageSize: 1024,
            getAdditionalData: true
        };
        var url = "/databases";

        var resultsSelector = (databases: databaseDto[]) => databases.map((db: databaseDto) =>
            new database(db.Name, db.IsAdminCurrentTenant, db.Disabled, db.Bundles, db.IndexingDisabled,
                db.RejectClientsEnabled, db.IsLoaded, db.ClusterWide, db.Stats));
        return this.query(url, args, null, resultsSelector);
    }
}

export = getDatabasesCommand;
