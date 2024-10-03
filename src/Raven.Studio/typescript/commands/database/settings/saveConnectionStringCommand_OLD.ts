import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import connectionStringRavenEtlModel = require("models/database/settings/connectionStringRavenEtlModel");
import connectionStringSqlEtlModel = require("models/database/settings/connectionStringSqlEtlModel");
import connectionStringSnowflakeEtlModel = require("models/database/settings/connectionStringSnowflakeEtlModel");
import connectionStringOlapEtlModel = require("models/database/settings/connectionStringOlapEtlModel");
import connectionStringElasticSearchEtlModel = require("models/database/settings/connectionStringElasticSearchEtlModel");
import connectionStringKafkaModel from "models/database/settings/connectionStringKafkaModel";
import connectionStringRabbitMqModel from "models/database/settings/connectionStringRabbitMqModel";
import connectionStringAzureQueueStorageModel from "models/database/settings/connectionStringAzureQueueStorageModel";

class saveConnectionStringCommand_OLD extends commandBase {

    constructor(private db: database, private connectionString: connectionStringRavenEtlModel |
        connectionStringSqlEtlModel |
        connectionStringSnowflakeEtlModel |
        connectionStringOlapEtlModel |
        connectionStringElasticSearchEtlModel |
        connectionStringKafkaModel |
        connectionStringRabbitMqModel |
        connectionStringAzureQueueStorageModel) {
        super();
    }
 
    execute(): JQueryPromise<void> { 
        return this.saveConnectionString()
            .fail((response: JQueryXHR) => this.reportError("Failed to save connection string", response.responseText, response.statusText))
            .done(() => this.reportSuccess(`Connection string was saved successfully`));
    }

    private saveConnectionString(): JQueryPromise<void> { 
        
        const url = endpoints.databases.ongoingTasks.adminConnectionStrings;
        
        const saveConnectionStringTask = $.Deferred<void>();
        
        const payload = this.connectionString.toDto();

        this.put(url, JSON.stringify(payload), this.db)
            .done(() => saveConnectionStringTask.resolve())
            .fail(response => saveConnectionStringTask.reject(response));

        return saveConnectionStringTask;
    }
}

export = saveConnectionStringCommand_OLD; 

