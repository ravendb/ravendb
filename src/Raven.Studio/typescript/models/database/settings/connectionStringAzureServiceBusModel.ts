/// <reference path="../../../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import connectionStringModel = require("models/database/settings/connectionStringModel");
import saveConnectionStringCommand_OLD = require("commands/database/settings/saveConnectionStringCommand_OLD");
import testAzureServiceBusServerConnectionCommand = require("commands/database/cluster/testAzureServiceBusServerConnectionCommand");
import jsonUtil = require("common/jsonUtil");

class connectionStringAzureServiceBusModel extends connectionStringModel {

    azureServiceBusConnectionString = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString, isNew: boolean, tasks: { taskName: string; taskId: number }[]) {
        super(isNew, tasks);

        this.update(dto);
        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.connectionStringName,
            this.azureServiceBusConnectionString
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    update(dto: Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString) {
        super.update(dto);

        const azureServiceBusSettings = dto.AzureServiceBusConnectionSettings;
        if (azureServiceBusSettings) {
            this.azureServiceBusConnectionString(azureServiceBusSettings.ConnectionString);
        }
    }

    initValidation() {
        super.initValidation();

        this.azureServiceBusConnectionString.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            azureServiceBusConnectionString: this.azureServiceBusConnectionString,
        });
    }

    static empty(): connectionStringAzureServiceBusModel {
        return new connectionStringAzureServiceBusModel({
            Type: "Queue",
            BrokerType: "AzureServiceBus",
            Name: "",

            AzureServiceBusConnectionSettings: {
                ConnectionString: "",
                EntraId: null,
                Passwordless: null,
            },

            KafkaConnectionSettings: null,
            RabbitMqConnectionSettings: null,
            AzureQueueStorageConnectionSettings: null,
            AmazonSqsConnectionSettings: null,
        }, true, []);
    }

    toDto(): Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString {
        return {
            Type: "Queue",
            BrokerType: "AzureServiceBus",
            Name: this.connectionStringName(),

            AzureServiceBusConnectionSettings: {
                ConnectionString: this.azureServiceBusConnectionString(),
                EntraId: null,
                Passwordless: null,
            },

            KafkaConnectionSettings: null,
            RabbitMqConnectionSettings: null,
            AzureQueueStorageConnectionSettings: null,
            AmazonSqsConnectionSettings: null,
        };
    }

    saveConnectionString(db: database): JQueryPromise<void> {
        return new saveConnectionStringCommand_OLD(db, this)
            .execute();
    }

    testConnection(db: database): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        return new testAzureServiceBusServerConnectionCommand(db, {
            ConnectionString: this.azureServiceBusConnectionString(),
            EntraId: null,
            Passwordless: null,
        }).execute();
    }
}

export = connectionStringAzureServiceBusModel;
