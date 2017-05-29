/// <reference path="../../../../typings/tsd.d.ts"/>

import configuration = require("configuration");
import clusterNode = require("models/database/cluster/clusterNode");

class databaseCreationModel {

    readonly configurationSections: Array<availableConfigurationSection> = [
        {
            name: "Encryption",
            alwaysEnabled: false,
            enabled: ko.observable<boolean>(false)
        },
        {
            name: "Replication",
            alwaysEnabled: true,
            enabled: ko.observable<boolean>(true)
        },
        {
            name: "Path",
            alwaysEnabled: true,
            enabled: ko.observable<boolean>(true)
        }
    ];

    name = ko.observable<string>("");

    replication = {
        replicationFactor: ko.observable<number>(2),
        manualMode: ko.observable<boolean>(false),
        nodes: ko.observableArray<clusterNode>([])
    }

    replicationValidationGroup = ko.validatedObservable({
        replicationFactor: this.replication.replicationFactor
    });

    path = {
        indexesPath: ko.observable<string>(),
        dataPath: ko.observable<string>(),
        journalsPath: ko.observable<string>(),
        tempPath: ko.observable<string>()
    }

    pathValidationGroup = ko.validatedObservable({
        dataPath: this.path.dataPath,
        journalsPath: this.path.journalsPath,
        tempPath: this.path.tempPath,
        indexesPath: this.path.indexesPath
    });

    encryption = {
        key: ko.observable<string>(),
        confirmation: ko.observable<boolean>(false)
    }
   
    encryptionValidationGroup = ko.validatedObservable({
        key: this.encryption.key,
        confirmation: this.encryption.confirmation
    });

    globalValidationGroup = ko.validatedObservable({
        name: this.name
    });

    constructor() {
        const encryptionConfig = this.getEncryptionConfigSection();
        encryptionConfig.validationGroup = this.encryptionValidationGroup;

        const replicationConfig = this.configurationSections.find(x => x.name === "Replication");
        replicationConfig.validationGroup = this.replicationValidationGroup;

        const pathConfig = this.configurationSections.find(x => x.name === "Path");
        pathConfig.validationGroup = this.pathValidationGroup;

        encryptionConfig.enabled.subscribe(enabled => {
            if (enabled) {
                this.replication.manualMode(true);
                this.replication.replicationFactor(this.replication.nodes().length);
            }
        });

        this.replication.nodes.subscribe(nodes => {
            this.replication.replicationFactor(nodes.length);
        });
    }

    getEncryptionConfigSection() {
        return this.configurationSections.find(x => x.name === "Encryption");
    }

    protected setupPathValidation(observable: KnockoutObservable<string>, name: string) {
        const maxLength = 248;

        const rg1 = /^[^*?"<>\|]*$/; // forbidden characters * ? " < > |
        const rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names

        observable.extend({
            maxLength: {
                params: maxLength,
                message: `Path name for '${name}' can't exceed ${maxLength} characters!`
            },
            validation: [{
                validator: (val: string) => rg1.test(val),
                message: `{0} path can't contain any of the following characters: * ? " < > |`,
                params: name
            },
            {
                validator: (val: string) => !rg3.test(val),
                message: `The name {0} is forbidden for use!`,
                params: this.name
            }]
        });
    }

    setupValidation(databaseDoesntExist: (name: string) => boolean, maxReplicationFactor: number) {
        const rg1 = /^[^\\/:\*\?"<>\|]*$/; // forbidden characters \ / : * ? " < > |
        const rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names

        this.setupPathValidation(this.path.dataPath, "Data");
        this.setupPathValidation(this.path.tempPath, "Temp");
        this.setupPathValidation(this.path.journalsPath, "Journals");

        this.replication.replicationFactor.extend({
            required: true,
            validation: [
                {
                    validator: (val: number) => val >= 1,
                    message: `Replication factor must be at least 1`
                },
                {
                    validator: (val: number) => val <= maxReplicationFactor,
                    message: `Max available nodes: {0}`,
                    params: maxReplicationFactor
                }]
        });

        this.name.extend({
            required: true,
            maxLength: 230,
            validation: [
                {
                    validator: databaseDoesntExist,
                    message: "Database already exists"
                }, {
                    validator: (val: string) => rg1.test(val),
                    message: `The database name can't contain any of the following characters: \\ / : * ? " < > |`,
                }, {
                    validator: (val: string) => !val.startsWith("."),
                    message: `The database name can't start with a dot!`
                }, {
                    validator: (val: string) => !val.endsWith("."),
                    message: `The database name can't end with a dot!`
                }, {
                    validator: (val: string) => !rg3.test(val),
                    message: `The name {0} is forbidden for use!`,
                    params: this.name
                }]
        });

        this.setupPathValidation(this.path.indexesPath, "Indexes");

        this.encryption.key.extend({
            required: true,
            base64: true //TODO: any other validaton ?
        });

        this.encryption.confirmation.extend({
            validation: [
                {
                    validator: (v: boolean) => v,
                    message: "Please confirm you saved encryption key"
                }
            ]
        });
    }

    private topologyToDto(): Raven.Client.Server.DatabaseTopology {
        if (this.replication.manualMode()) {
            const nodes = this.replication.nodes();
            return {
                Members: nodes.map(node => ({
                    Database: this.name(),
                    NodeTag: node.tag(),
                    Url: node.serverUrl()
                }))
            } as Raven.Client.Server.DatabaseTopology;
        }
        return undefined;
    }

    toDto(): Raven.Client.Server.DatabaseRecord {
        const settings: dictionary<string> = {};
        const securedSettings: dictionary<string> = {};

        if (this.path.tempPath() && this.path.tempPath().trim()) {
            settings[configuration.storage.tempPath] = this.path.tempPath();
        }

        if (this.path.dataPath() && this.path.dataPath().trim) {
            settings[configuration.core.dataDirectory] = this.path.dataPath();
        }

        if (this.path.indexesPath() && this.path.indexesPath().trim()) {
            settings[configuration.indexing.storagePath] = this.path.indexesPath();
        }

        if (this.path.journalsPath() && this.path.journalsPath().trim()) {
            settings[configuration.storage.journalsStoragePath] = this.path.journalsPath();
        }

        return {
            DatabaseName: this.name(),
            Settings: settings,
            SecuredSettings: securedSettings,
            Disabled: false,
            Encrypted: this.getEncryptionConfigSection().enabled(),
            Topology: this.topologyToDto()
        } as Raven.Client.Server.DatabaseRecord;
    }

}

export = databaseCreationModel;
