import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getConnectionStringInfoCommand extends commandBase {
    private constructor(private db: database | string, private type: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType, private connectionStringName: string) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult> {
        return this.getConnectionStringInfo()
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get info for connection string: ${this.connectionStringName}`, response.responseText, response.statusText);
            });
    }

    private getConnectionStringInfo(): JQueryPromise<Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult> {
        const args = { connectionStringName: this.connectionStringName, type: this.type };
        const url = endpoints.databases.ongoingTasks.adminConnectionStrings + this.urlEncodeArgs(args);

        return this.query(url, null, this.db);
    }

    static forRavenEtl(db: database | string, connectionStringName: string) {
        return new getConnectionStringInfoCommand(db, "Raven", connectionStringName);
    }

    static forSqlEtl(db: database | string, connectionStringName: string) {
        return new getConnectionStringInfoCommand(db, "Sql", connectionStringName);
    }

    static forSnowflakeEtl(db: database | string, connectionStringName: string) {
        return new getConnectionStringInfoCommand(db, "Snowflake", connectionStringName);
    }

    static forOlapEtl(db: database | string, connectionStringName: string) {
        return new getConnectionStringInfoCommand(db, "Olap", connectionStringName);
    }

    static forElasticSearchEtl(db: database | string, connectionStringName: string) {
        return new getConnectionStringInfoCommand(db, "ElasticSearch", connectionStringName);
    }

    static forKafkaEtl(db: database | string, connectionStringName: string) {
        return new getConnectionStringInfoCommand(db, "Queue", connectionStringName);
    }

    static forRabbitMqEtl(db: database | string, connectionStringName: string) {
        return new getConnectionStringInfoCommand(db, "Queue", connectionStringName);
    }
    
    static forAzureQueueStorageEtl(db: database, connectionStringName: string) {
        return new getConnectionStringInfoCommand(db, "Queue", connectionStringName);
    }
}

export = getConnectionStringInfoCommand; 
