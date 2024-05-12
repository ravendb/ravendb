/// <reference path="../../../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import connectionStringModel = require("models/database/settings/connectionStringModel");
import saveConnectionStringCommand_OLD = require("commands/database/settings/saveConnectionStringCommand_OLD");
import testRabbitMqServerConnectionCommand from "commands/database/cluster/testRabbitMqServerConnectionCommand";
import jsonUtil = require("common/jsonUtil");

class connectionStringRabbitMqModel extends connectionStringModel {
    
    rabbitMqConnectionString = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup;    
    dirtyFlag: () => DirtyFlag;
    
    constructor(dto: Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString, isNew: boolean, tasks: { taskName: string; taskId: number }[]) {
        super(isNew, tasks);
        
        this.update(dto);
        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.connectionStringName,
            this.rabbitMqConnectionString
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    update(dto: Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString) {
        super.update(dto);
        
        const rabbitSettings = dto.RabbitMqConnectionSettings;
        this.rabbitMqConnectionString(rabbitSettings.ConnectionString);
    }

    initValidation() {
        super.initValidation();
        
        this.rabbitMqConnectionString.extend({
            required: true
        });
       
        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            rabbitMqConnectionString: this.rabbitMqConnectionString,
        });
    }

    static empty(): connectionStringRabbitMqModel {
        return new connectionStringRabbitMqModel({
            Type: "Queue",
            BrokerType: "RabbitMq",
            Name: "",
            
            RabbitMqConnectionSettings: {
                ConnectionString: ""
            },
            
            KafkaConnectionSettings: null,
            AzureQueueStorageConnectionSettings: null,
        }, true, []);
    }
    
    toDto(): Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString  {
        return {
            Type: "Queue",
            BrokerType: "RabbitMq",
            Name: this.connectionStringName(),
            
            RabbitMqConnectionSettings: {
                ConnectionString: this.rabbitMqConnectionString()
            },

            KafkaConnectionSettings: null,
            AzureQueueStorageConnectionSettings: null,
        };
    }

    saveConnectionString(db: database) : JQueryPromise<void> {
        return new saveConnectionStringCommand_OLD(db, this)
            .execute();
    }

    testConnection(db: database): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        return new testRabbitMqServerConnectionCommand(db, this.rabbitMqConnectionString())
            .execute();
    }
}

export = connectionStringRabbitMqModel;
