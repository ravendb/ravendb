/// <reference path="../../../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import connectionStringModel = require("models/database/settings/connectionStringModel");
import saveConnectionStringCommand = require("commands/database/settings/saveConnectionStringCommand");
import testKafkaServerConnectionCommand from "commands/database/cluster/testKafkaServerConnectionCommand";
import jsonUtil = require("common/jsonUtil");

class connectionOptionModel {
    key = ko.observable<string>();
    value = ko.observable<string>();
    
    isValidKeyValue = ko.observable<boolean>();

    validationGroup: KnockoutObservable<any>;
    dirtyFlag: () => DirtyFlag;

    constructor(key: string, value: string) {
        this.key(key);
        this.value(value);

        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.key,
            this.value,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    private initValidation() {
        this.isValidKeyValue.extend({
           validation: [
               {
                   validator: () => this.key() && this.value(),
                   message: "Both key and value are required"
               } 
           ] 
        });

        this.validationGroup = ko.validatedObservable({
            isValidKeyValue: this.isValidKeyValue
        });
    }

    static empty() {
        return new connectionOptionModel("", "");
    }
}

class connectionStringKafkaEtlModel extends connectionStringModel {
    
    kafkaServerUrl = ko.observable<string>(); // TODO rename to bootstrapServers...
    useRavenCertificate = ko.observable<boolean>(); // TODO show only if secure server...
    connectionOptions = ko.observableArray<connectionOptionModel>();
    
    validationGroup: KnockoutValidationGroup;
    hasTestError = ko.observable<boolean>(false)
    
    dirtyFlag: () => DirtyFlag;

    spinners = {
        testUrl: ko.observable<boolean>(false)
    };
    
    constructor(dto: Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString, isNew: boolean, tasks: { taskName: string; taskId: number }[]) {
        super(isNew, tasks);
        
        this.update(dto);
        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.kafkaServerUrl,
            this.useRavenCertificate,
            this.connectionStringName,
            this.connectionOptions
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    update(dto: Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString) {
        super.update(dto);
        
        const kafkaSettings = dto.KafkaConnectionSettings;
        this.kafkaServerUrl(kafkaSettings.BootstrapServers);
        this.useRavenCertificate(kafkaSettings.UseRavenCertificate);

        _.forIn(kafkaSettings.ConnectionOptions, (value, key) => {
            this.connectionOptions.push(new connectionOptionModel(key, value));
        });
    }

    initValidation() {
        super.initValidation();
        
        this.kafkaServerUrl.extend({
            required: true
        });
       
        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            kafkaServerUrl: this.kafkaServerUrl
        });
    }

    static empty(): connectionStringKafkaEtlModel {
        return new connectionStringKafkaEtlModel({
            Type: "Queue",
            BrokerType: "Kafka",
            Name: "",

            KafkaConnectionSettings: {
                BootstrapServers: "",
                UseRavenCertificate: false,
                ConnectionOptions: null,
            },
            
            RabbitMqConnectionSettings: null
        }, true, []);
    }
    
    toDto(): Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString  {
        return {
            Type: "Queue",
            BrokerType: "Kafka",
            Name: this.connectionStringName(),
            
            KafkaConnectionSettings: {
                BootstrapServers: this.kafkaServerUrl(),
                UseRavenCertificate: this.useRavenCertificate(),
                ConnectionOptions: this.connectionOptionsToDto()
            },
            
            RabbitMqConnectionSettings: null
        };
    }

    connectionOptionsToDto() {
        const result = {} as {[key: string]: string;}; 

        this.connectionOptions().forEach((item: connectionOptionModel) => {
            result[item.key()] = item.value();
        });

        return result;
    }

    saveConnectionString(db: database) : JQueryPromise<void> {
        return new saveConnectionStringCommand(db, this)
            .execute();
    }

    removeConnectionOption(item: connectionOptionModel) {
        this.connectionOptions.remove(item);
    }

    addNewConnectionOption() {
        this.connectionOptions.push(connectionOptionModel.empty());
    }

    testConnection(db: database): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        return new testKafkaServerConnectionCommand(db, this.kafkaServerUrl(), this.useRavenCertificate(), this.connectionOptionsToDto())
            .execute()
            .done((result) => {
                if (result.Error) {
                    this.hasTestError(true); // TODO handle in UI
                }
            });
    }
}

export = connectionStringKafkaEtlModel;
