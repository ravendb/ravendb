import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import connectionStringRavenEtlModel = require("models/database/settings/connectionStringRavenEtlModel");
import connectionStringSqlEtlModel = require("models/database/settings/connectionStringSqlEtlModel");

class saveConnectionStringCommand extends commandBase {
    private ravenEtlConnectionStringToSend: Raven.Client.ServerWide.ETL.RavenConnectionString;
    private sqlEtlConnectionStringToSend: Raven.Client.ServerWide.ETL.SqlConnectionString;

    constructor(private db: database, private connectionStringType: Raven.Client.ServerWide.ConnectionStringType,
                private ravenEtlConnectionString: connectionStringRavenEtlModel, private sqlEtlConnectionString: connectionStringSqlEtlModel) {
        super();

        switch (connectionStringType) {
            case "Raven":
            {
                this.ravenEtlConnectionStringToSend = {
                    Type: "Raven",
                    Name: ravenEtlConnectionString.connectionStringName(),
                    Url: ravenEtlConnectionString.url(),
                    Database: ravenEtlConnectionString.database()
                };
            } break;
            case "Sql":
            {
                    this.sqlEtlConnectionStringToSend = {
                    Type: "Sql",
                    Name: sqlEtlConnectionString.connectionStringName(),
                    ConnectionString: sqlEtlConnectionString.connectionString()
                };
            } break;
        }
    }
 
    execute(): JQueryPromise<void> { 
        return this.saveConnectionString()
            .fail((response: JQueryXHR) => this.reportError("Failed to save connection string", response.responseText, response.statusText))
            .done(() => this.reportSuccess(`Connection string was saved successfully`));
    }

    private saveConnectionString(): JQueryPromise<void> { 
        
        const args = { name: this.db.name };
        const url = endpoints.global.adminDatabases.adminConnectionStrings + this.urlEncodeArgs(args);
        
        const saveConnectionStringTask = $.Deferred<void>();

        const payload =  this.connectionStringType === "Raven" ? this.ravenEtlConnectionStringToSend : this.sqlEtlConnectionStringToSend ;

        this.put(url, JSON.stringify(payload))
            .done((results: Array<void>) => { 
                saveConnectionStringTask.resolve(results[0]);
            })
            .fail(response => saveConnectionStringTask.reject(response));

        return saveConnectionStringTask;
    }
}

export = saveConnectionStringCommand; 

