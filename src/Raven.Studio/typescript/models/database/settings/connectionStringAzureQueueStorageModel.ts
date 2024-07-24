/// <reference path="../../../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import connectionStringModel = require("models/database/settings/connectionStringModel");
import saveConnectionStringCommand_OLD = require("commands/database/settings/saveConnectionStringCommand_OLD");
import jsonUtil = require("common/jsonUtil");
import testAzureQueueStorageServerConnectionCommand
    from "commands/database/cluster/testAzureQueueStorageServerConnectionCommand";
import assertUnreachable from "components/utils/assertUnreachable";

class AzureQueueStorageConnectionStringModel {
    connectionString = ko.observable<string>();
    
    onChange(action: () => void) {
        this.connectionString.subscribe(action);
    }
    
    initValidation(condition: () => boolean) {
        this.connectionString.extend({
            required: {
                onlyIf: condition
            },
            validation: {
                validator: (value: string) => {
                    if (!value) {
                        return true;
                    }
                    
                    return value.includes("DefaultEndpointsProtocol") &&
                        value.includes("AccountName") &&
                        value.includes("AccountKey") &&
                        value.includes("QueueEndpoint")
                },
                message: "Please define all required fields: DefaultEndpointsProtocol, AccountName, AccountKey and QueueEndpoint"
            }
        });
    }
    
    toDto(): Raven.Client.Documents.Operations.ETL.Queue.AzureQueueStorageConnectionSettings {
        const connectionStringWithoutNewLines = this.connectionString().replace(/\n/g, "");

        return {
            ConnectionString: connectionStringWithoutNewLines,
            EntraId: null,
            Passwordless: null
        }
    }
    
    update(connectionString: string) {
        this.connectionString(connectionString);
    }
}

class AzureQueueStorageEntraIdModel {
    clientId = ko.observable<string>();
    clientSecret = ko.observable<string>();
    storageAccountName = ko.observable<string>();
    tenantId = ko.observable<string>();

    onChange(action: () => void) {
        this.clientId.subscribe(action);
        this.clientSecret.subscribe(action);
        this.storageAccountName.subscribe(action);
        this.tenantId.subscribe(action);
    }

    initValidation(condition: () => boolean) {
        this.clientId.extend({
            required: {
                onlyIf: condition
            }
        });
        this.clientSecret.extend({
            required: {
                onlyIf: condition
            }
        });
        this.storageAccountName.extend({
            required: {
                onlyIf: condition
            }
        });
        this.tenantId.extend({
            required: {
                onlyIf: condition
            }
        });
    }
    
    toDto(): Raven.Client.Documents.Operations.ETL.Queue.AzureQueueStorageConnectionSettings {
        return {
            ConnectionString: null,
            EntraId: {
                ClientId: this.clientId(),
                ClientSecret: this.clientSecret(),
                StorageAccountName: this.storageAccountName(),
                TenantId: this.tenantId(),
            },
            Passwordless: null
        }
    }

    update(dto: Raven.Client.Documents.Operations.ETL.Queue.EntraId) {
        this.clientId(dto.ClientId);
        this.clientSecret(dto.ClientSecret);
        this.storageAccountName(dto.StorageAccountName);
        this.tenantId(dto.TenantId);
    }
}

class AzureQueueStoragePasswordlessModel {

    storageAccountName = ko.observable<string>();
    
    onChange(action: () => void) {
        this.storageAccountName.subscribe(action);
    }

    initValidation(condition: () => boolean) {
        this.storageAccountName.extend({
            required: {
                onlyIf: condition
            }
        });
    }
    
    toDto(): Raven.Client.Documents.Operations.ETL.Queue.AzureQueueStorageConnectionSettings {
        return {
            ConnectionString: null,
            EntraId: null,
            Passwordless: {
                StorageAccountName: this.storageAccountName()
            }
        }
    }

    update(dto: Raven.Client.Documents.Operations.ETL.Queue.Passwordless) {
        this.storageAccountName(dto.StorageAccountName);
    }
}

class connectionStringAzureQueueStorageModel extends connectionStringModel {

    authenticationType = ko.observable<AzureQueueStorageAuthenticationType>("connectionString");
    
    connectionStringModel = new AzureQueueStorageConnectionStringModel();
    entraIdModel = new AzureQueueStorageEntraIdModel();
    passwordlessModel = new AzureQueueStoragePasswordlessModel();

    validationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString, isNew: boolean, tasks: { taskName: string; taskId: number }[]) {
        super(isNew, tasks);

        this.update(dto);
        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.connectionStringName,
            this.authenticationType,
            this.connectionStringModel.connectionString,
            this.entraIdModel.tenantId,
            this.entraIdModel.storageAccountName,
            this.entraIdModel.clientSecret,
            this.entraIdModel.clientId,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    formatAuthenticationType(authenticationType: AzureQueueStorageAuthenticationType) {
        switch (authenticationType) {
            case "connectionString":
                return "Connection String";
            case "entraId":
                return "Entra ID";
            case "passwordless":
                return "Passwordless";
            default:
                assertUnreachable(authenticationType);
        }
    }
    
    onChange(action: () => void) {
        this.authenticationType.subscribe(action);
        this.connectionStringModel.onChange(action);
        this.entraIdModel.onChange(action);
        this.passwordlessModel.onChange(action);
    }

    update(dto: Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString) {
        super.update(dto);

        const settings = dto.AzureQueueStorageConnectionSettings;
        if (settings.Passwordless) {
            this.authenticationType("passwordless");
            this.passwordlessModel.update(settings.Passwordless);
        } else if (settings.ConnectionString) {
            this.authenticationType("connectionString");
            this.connectionStringModel.update(settings.ConnectionString);
        } else if (settings.EntraId) {
            this.authenticationType("entraId");
            this.entraIdModel.update(settings.EntraId);
        }
    }

    initValidation() {
        super.initValidation();
        
        this.connectionStringModel.initValidation(() => this.authenticationType() === "connectionString");
        this.entraIdModel.initValidation(() => this.authenticationType() === "entraId");
        this.passwordlessModel.initValidation(() => this.authenticationType() === "passwordless");

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            connectionStringConfigurationConnectionString: this.connectionStringModel.connectionString,
            entraIdClientId: this.entraIdModel.clientId,
            entraIdClientSecret: this.entraIdModel.clientSecret,
            entraIdStorageAccountName: this.entraIdModel.storageAccountName,
            entraIdTenantId: this.entraIdModel.tenantId,
            passwordLessStorageAccountName: this.passwordlessModel.storageAccountName,
        });
    }

    static empty(): connectionStringAzureQueueStorageModel {
        return new connectionStringAzureQueueStorageModel({
            Type: "Queue",
            BrokerType: "AzureQueueStorage",
            Name: "",

            RabbitMqConnectionSettings: null,
            KafkaConnectionSettings: null,
            AzureQueueStorageConnectionSettings: {
                ConnectionString: "",
                EntraId: null,
                Passwordless: null,
            },
        }, true, []);
    }
    
    private authenticationToDto(): Raven.Client.Documents.Operations.ETL.Queue.AzureQueueStorageConnectionSettings {
        const authenticationType = this.authenticationType();
        switch (authenticationType) {
            case "connectionString": 
                return this.connectionStringModel.toDto();
            case "entraId":
                return this.entraIdModel.toDto();
            case "passwordless":
                return this.passwordlessModel.toDto();
            default:
                assertUnreachable(authenticationType);
        }
    }
    
    toDto(): Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString  {
        return {
            Type: "Queue",
            BrokerType: "AzureQueueStorage",
            Name: this.connectionStringName(),
            RabbitMqConnectionSettings: null,
            KafkaConnectionSettings: null,
            AzureQueueStorageConnectionSettings: this.authenticationToDto()
        };
    }

    saveConnectionString(db: database) : JQueryPromise<void> {
        return new saveConnectionStringCommand_OLD(db, this)
            .execute();
    }

    testConnection(db: database): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        return new testAzureQueueStorageServerConnectionCommand(db, this.authenticationToDto())
            .execute();
    }
}

export = connectionStringAzureQueueStorageModel;
