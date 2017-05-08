import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");

class saveVersioningCommand extends commandBase {
    constructor(private db: database, private versioningConfiguration: Raven.Client.Server.Versioning.VersioningConfiguration) {
        super();
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {

        const url = endpoint.global.adminDatabases.adminVersioningConfig + this.urlEncodeArgs({ name: this.db.name });
        const args = ko.toJSON(this.versioningConfiguration);
        return this.post<updateDatabaseConfigurationsResult>(url, args);

    }
}

export = saveVersioningCommand;
