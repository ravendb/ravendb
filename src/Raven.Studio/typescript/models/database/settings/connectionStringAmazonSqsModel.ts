/// <reference path="../../../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import connectionStringModel = require("models/database/settings/connectionStringModel");
import saveConnectionStringCommand_OLD = require("commands/database/settings/saveConnectionStringCommand_OLD");
import jsonUtil = require("common/jsonUtil");
import assertUnreachable from "components/utils/assertUnreachable";
import testAmazonSqsServerConnectionCommand from "commands/database/cluster/testAmazonSqsServerConnectionCommand";

class AmazonSqsBasicModel {
    accessKey = ko.observable<string>();
    secretKey = ko.observable<string>();
    regionName = ko.observable<string>();

    onChange(action: () => void) {
        this.accessKey.subscribe(action);
        this.secretKey.subscribe(action);
        this.regionName.subscribe(action);
    }

    initValidation(condition: () => boolean) {
        this.accessKey.extend({
            required: {
                onlyIf: condition
            }
        });
        this.secretKey.extend({
            required: {
                onlyIf: condition
            }
        });
        this.regionName.extend({
            required: {
                onlyIf: condition
            }
        });
    }
    
    toDto(): Raven.Client.Documents.Operations.ETL.Queue.AmazonSqsConnectionSettings {
        return {
            Basic: {
                AccessKey: this.accessKey(),
                RegionName: this.regionName(),
                SecretKey: this.secretKey()
            },
            Passwordless: false
        }
    }

    update(dto: Raven.Client.Documents.Operations.ETL.Queue.Basic) {
        this.accessKey(dto.AccessKey);
        this.secretKey(dto.SecretKey);
        this.regionName(dto.RegionName);
    }
}


class connectionStringAmazonSqsModel extends connectionStringModel {

    authenticationType = ko.observable<AmazonSqsAuthenticationType>("basic");
    
    basicModel = new AmazonSqsBasicModel();

    validationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString, isNew: boolean, tasks: { taskName: string; taskId: number }[]) {
        super(isNew, tasks);

        this.update(dto);
        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.connectionStringName,
            this.authenticationType,
            this.basicModel.accessKey,
            this.basicModel.secretKey,
            this.basicModel.regionName,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    formatAuthenticationType(authenticationType: AmazonSqsAuthenticationType) {
        switch (authenticationType) {
            case "basic":
                return "Basic";
            case "passwordless":
                return "Passwordless";
            default:
                assertUnreachable(authenticationType);
        }
    }
    
    onChange(action: () => void) {
        this.authenticationType.subscribe(action);
        this.basicModel.onChange(action);
    }

    update(dto: Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString) {
        super.update(dto);

        const settings = dto.AmazonSqsConnectionSettings;
        if (settings.Passwordless) {
            this.authenticationType("passwordless");
        } else if (settings.Basic) {
            this.authenticationType("basic");
            this.basicModel.update(settings.Basic);
        }
    }

    initValidation() {
        super.initValidation();
        
        this.basicModel.initValidation(() => this.authenticationType() === "basic");

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            basicAccessKey: this.basicModel.accessKey,
            basicSecretKey: this.basicModel.secretKey,
            basicRegionName: this.basicModel.regionName
        });
    }

    static empty(): connectionStringAmazonSqsModel {
        return new connectionStringAmazonSqsModel({
            Type: "Queue",
            BrokerType: "AmazonSqs",
            Name: "",
            RabbitMqConnectionSettings: null,
            KafkaConnectionSettings: null,
            AzureQueueStorageConnectionSettings: null,
            AmazonSqsConnectionSettings: {
                Basic: {
                    AccessKey: "",
                    SecretKey: "",
                    RegionName: ""
                },
                Passwordless: false,
            }
        }, true, []);
    }
    
    private authenticationToDto(): Raven.Client.Documents.Operations.ETL.Queue.AmazonSqsConnectionSettings {
        const authenticationType = this.authenticationType();
        switch (authenticationType) {
            case "basic":
                return this.basicModel.toDto();
            case "passwordless":
                return {
                    Basic: null,
                    Passwordless: true,
                }
            default:
                assertUnreachable(authenticationType);
        }
    }
    
    toDto(): Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString  {
        return {
            Type: "Queue",
            BrokerType: "AmazonSqs",
            Name: this.connectionStringName(),
            RabbitMqConnectionSettings: null,
            KafkaConnectionSettings: null,
            AzureQueueStorageConnectionSettings: null,
            AmazonSqsConnectionSettings: this.authenticationToDto()
        };
    }

    saveConnectionString(db: database) : JQueryPromise<void> {
        return new saveConnectionStringCommand_OLD(db, this)
            .execute();
    }

    testConnection(db: database): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        return new testAmazonSqsServerConnectionCommand(db, this.authenticationToDto())
            .execute();
    }
}

export = connectionStringAmazonSqsModel;
