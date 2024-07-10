/// <reference path="../../../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import connectionStringModel = require("models/database/settings/connectionStringModel");
import saveConnectionStringCommand_OLD = require("commands/database/settings/saveConnectionStringCommand_OLD");
import testKafkaServerConnectionCommand from "commands/database/cluster/testKafkaServerConnectionCommand";
import accessManager = require("common/shell/accessManager");
import jsonUtil = require("common/jsonUtil");

class connectionOptionModel {
    
    static multiLineKeys: string[] = ["ssl.keystore.key", "ssl.keystore.certificate.chain", "ssl.truststore.certificates", "ssl.key.pem", "ssl.certificate.pem", "ssl.ca.pem"]; 
    
    key = ko.observable<string>();
    value = ko.observable<string>();
    
    multiLine: KnockoutComputed<boolean>;
    
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
        
        this.multiLine = ko.pureComputed(() => connectionOptionModel.multiLineKeys.includes(this.key()));
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

class connectionStringKafkaModel extends connectionStringModel {
    
    bootstrapServers = ko.observable<string>();
    useRavenCertificate = ko.observable<boolean>();
    connectionOptions = ko.observableArray<connectionOptionModel>();

    isSecureServer = accessManager.default.secureServer();
    static readonly sslCaLocation: string = "ssl.ca.location";
    
    validationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    spinners = {
        testUrl: ko.observable<boolean>(false)
    };
    
    constructor(dto: Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString, isNew: boolean, tasks: { taskName: string; taskId: number }[]) {
        super(isNew, tasks);
        
        this.update(dto);
        this.initValidation();
        
        this.useRavenCertificate.subscribe(toggledOn => {
            if (toggledOn && !this.connectionOptions().find(x => x.key() === connectionStringKafkaModel.sslCaLocation)) {
                this.connectionOptions.unshift(new connectionOptionModel(connectionStringKafkaModel.sslCaLocation, ""));
            }
        });
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.bootstrapServers,
            this.useRavenCertificate,
            this.connectionStringName,
            this.connectionOptions
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    update(dto: Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString) {
        super.update(dto);
        
        const kafkaSettings = dto.KafkaConnectionSettings;
        this.bootstrapServers(kafkaSettings.BootstrapServers);
        this.useRavenCertificate(kafkaSettings.UseRavenCertificate);

        if (kafkaSettings.ConnectionOptions) {
            Object.entries(kafkaSettings.ConnectionOptions)
                .forEach(([key, value]) => {
                    this.connectionOptions.push(new connectionOptionModel(key, value));
                });
        }
    }

    initValidation() {
        super.initValidation();
        
        this.bootstrapServers.extend({
            required: true,
            validation: [
                {
                    validator: () => this.validateNoProtocol(),
                    message: "A bootstrap server cannot start with http/https"
                },
                {
                    validator: () => this.validateFormat(),
                    message: "Format should be: 'hostA:portNumber,hostB:portNumber,...'"
                }
            ]
        });
       
        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            bootstrapServers: this.bootstrapServers
        });
    }
    
    private validateNoProtocol(): boolean {
        const serverList = this.bootstrapServers().split(",");
        const serversWithProtocol = serverList.filter(x => x.startsWith("http") || x.startsWith('https'));
        
        return serversWithProtocol.length === 0;
    }

    private validateFormat(): boolean {
        const serverList = this.bootstrapServers().split(",");
        const hostPortRegexp = /^[a-zA-Z0-9\-_.]+:\d+$/;
        let result = true;
        
        serverList.forEach(x => {
            if (!hostPortRegexp.test(x)) {
                result = false;
            }
        })
        
        return result;
    }

    static empty(): connectionStringKafkaModel {
        return new connectionStringKafkaModel({
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
                BootstrapServers: this.bootstrapServers().trim(),
                UseRavenCertificate: this.useRavenCertificate(),
                ConnectionOptions: this.connectionOptionsToDto()
            },
            
            RabbitMqConnectionSettings: null
        };
    }

    connectionOptionsToDto(): Record<string, string> {
        const result: Record<string, string> = {}; 

        this.connectionOptions().forEach((item: connectionOptionModel) => {
            result[item.key()] = item.value();
        });

        return result;
    }

    saveConnectionString(db: database) : JQueryPromise<void> {
        return new saveConnectionStringCommand_OLD(db, this)
            .execute();
    }

    removeConnectionOption(item: connectionOptionModel) {
        this.connectionOptions.remove(item);
    }

    addNewConnectionOption() {
        this.connectionOptions.push(connectionOptionModel.empty());
    }

    testConnection(db: database): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        return new testKafkaServerConnectionCommand(db, this.bootstrapServers(), this.useRavenCertificate(), this.connectionOptionsToDto())
            .execute();
    }

    static readonly usingServerCertificateInfo =
        `<small><div class="margin-top-sm">The following <strong>configuration options</strong> will be set for you when using RavenDB server certificate:</div>
             <ul class="margin-top margin-top-xs no-padding-left margin-left-lg">
                 <li><code>security.protocol = SSL</code></li>
                 <li><code>ssl.key.pem = &lt;RavenDB Server Private Key&gt;</code></li>
                 <li><code>ssl.certificate.pem = &lt;RavenDB Server Public Key&gt;</code></li>
            </ul>
         </small>`;
}

export = connectionStringKafkaModel;
