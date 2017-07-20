/// <reference path="../../../../typings/tsd.d.ts"/>

import configuration = require("configuration");
import clusterNode = require("models/database/cluster/clusterNode");
import getRestorePointsCommand = require("commands/resources/getRestorePointsCommand");
import viewHelpers = require("common/helpers/view/viewHelpers");
import recentError = require("common/notifications/models/recentError");

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

    spinners = {
        fetchingRestorePoints: ko.observable<boolean>(false)
    }

    name = ko.observable<string>("");

    isFromBackup: boolean = false;
    backupDirectory = ko.observable<string>();
    isFocusOnBackupDirectory = ko.observable<boolean>();
    backupDirectoryError = ko.observable<string>(null);
    lastFailedBackupDirectory: string = null;
    restorePoints = ko.observableArray<Raven.Server.Documents.PeriodicBackup.RestorePoint>([]);
    selectedRestorePoint = ko.observable<string>();
    backupLocation = ko.observable<string>();
    lastFileNameToRestore = ko.observable<string>();

    replication = {
        replicationFactor: ko.observable<number>(2),
        manualMode: ko.observable<boolean>(false),
        nodes: ko.observableArray<clusterNode>([])
    }

    replicationValidationGroup = ko.validatedObservable({
        replicationFactor: this.replication.replicationFactor,
        nodes: this.replication.nodes
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
        name: this.name,
        selectedRestorePoint: this.selectedRestorePoint
    });

    // validation group to know if we can download/print/copy to clipboard the key
    saveKeyValidationGroup = ko.validatedObservable({
        name: this.name,
        key: this.encryption.key
    });

    backupDirectoryValidationGroup = ko.validatedObservable({
        backupDirectory: this.backupDirectory
    });

    constructor() {
        const encryptionConfig = this.getEncryptionConfigSection();
        encryptionConfig.validationGroup = this.encryptionValidationGroup;

        const replicationConfig = this.configurationSections.find(x => x.name === "Replication");
        replicationConfig.validationGroup = this.replicationValidationGroup;

        const pathConfig = this.configurationSections.find(x => x.name === "Path");
        pathConfig.validationGroup = this.pathValidationGroup;

        this.replication.nodes.subscribe(nodes => {
            this.replication.replicationFactor(nodes.length);
        });

        let isFirst = true;
        this.isFocusOnBackupDirectory.subscribe(hasFocus => {
            if (isFirst) {
                isFirst = false;
                return;
            }

            if (!this.isFromBackup)
                return;

            if (hasFocus)
                return;

            if (!viewHelpers.isValid(this.backupDirectoryValidationGroup) &&
                this.backupDirectory() === this.lastFailedBackupDirectory)
                return;

            this.spinners.fetchingRestorePoints(true);
            new getRestorePointsCommand(this.backupDirectory())
                .execute()
                .done((restorePoints: Raven.Server.Documents.PeriodicBackup.RestorePoints) => {
                    this.restorePoints(restorePoints.List);
                    this.selectedRestorePoint(null);
                    this.backupLocation(null);
                    this.lastFileNameToRestore(null);
                    this.backupDirectoryError(null);
                    this.lastFailedBackupDirectory = null;
                })
                .fail((response: JQueryXHR) => {
                    const messageAndOptionalException = recentError.tryExtractMessageAndException(response.responseText);
                    this.backupDirectoryError(recentError.trimMessage(messageAndOptionalException.message));
                    this.lastFailedBackupDirectory = this.backupDirectory();
                    this.backupDirectory.valueHasMutated();
                })
                .always(() => this.spinners.fetchingRestorePoints(false));
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

        this.setupPathValidation(this.path.dataPath, "Data");
        this.setupPathValidation(this.path.tempPath, "Temp");
        this.setupPathValidation(this.path.journalsPath, "Journals");

        this.replication.nodes.extend({
            validation: [{
                validator: (val: Array<clusterNode>) => !this.replication.manualMode() || this.replication.replicationFactor() > 0,
                message: `Please select at least one node.`
            }]
        });

        this.replication.replicationFactor.extend({
            required: true,
            validation: [
                {
                    validator: (val: number) => val >= 1 || this.replication.manualMode(),
                    message: `Replication factor must be at least 1.`
                },
                {
                    validator: (val: number) => val <= maxReplicationFactor,
                    message: `Max available nodes: {0}`,
                    params: maxReplicationFactor
                }
            ]
        });

        this.name.extend({
            required: true,
            maxLength: 230,
            validDatabaseName: true,

            validation: [
                {
                    validator: databaseDoesntExist,
                    message: "Database already exists"
                }
            ]
        });

        this.backupDirectory.extend({
            required: {
                onlyIf: () => this.isFromBackup && this.restorePoints().length === 0
            },
            validation: [
                {
                    validator: (_: string) => {
                        const result = this.isFromBackup && !this.backupDirectoryError();
                        return result;
                    },
                    message: "Couldn't fetch restore points, {0}",
                    params: this.backupDirectoryError
                }
            ]
        });

        this.selectedRestorePoint.extend({
            required: {
                onlyIf: () => this.isFromBackup
            }
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
                    message: "Please confirm that you have saved the encryption key"
                }
            ]
        });
    }

    private topologyToDto(): Raven.Client.Server.DatabaseTopology {
        if (this.replication.manualMode()) {
            const nodes = this.replication.nodes();
            return {
                Members: nodes.map(node => node.tag())
            } as Raven.Client.Server.DatabaseTopology;
        }
        return undefined;
    }

    useRestorePoint(restorePoint: Raven.Server.Documents.PeriodicBackup.RestorePoint) {
        this.selectedRestorePoint(restorePoint.Key);
        this.backupLocation(restorePoint.Details.Location);
        this.lastFileNameToRestore(restorePoint.Details.FileName);
    }

    toDto(): Raven.Client.Server.DatabaseRecord {
        const settings: dictionary<string> = {};
        const securedSettings: dictionary<string> = {};

        settings[configuration.core.dataDirectory] = _.trim(this.path.dataPath()) || null;
        settings[configuration.storage.tempPath] = _.trim(this.path.tempPath()) || null;
        settings[configuration.storage.journalsStoragePath] = _.trim(this.path.journalsPath()) || null;
        settings[configuration.indexing.storagePath] = _.trim(this.path.indexesPath()) || null;

        return {
            DatabaseName: this.name(),
            Settings: settings,
            SecuredSettings: securedSettings,
            Disabled: false,
            Encrypted: this.getEncryptionConfigSection().enabled(),
            Topology: this.topologyToDto()
        } as Raven.Client.Server.DatabaseRecord;
    }

    toRestoreDocumentDto(): Raven.Client.Server.PeriodicBackup.RestoreBackupConfiguration {
        const dataDirectory = _.trim(this.path.dataPath()) || null;
        const tempPath = _.trim(this.path.tempPath()) || null;
        const journalsStoragePath = _.trim(this.path.journalsPath()) || null;
        const indexingStoragePath = _.trim(this.path.indexesPath()) || null;

        return {
            DatabaseName: this.name(),
            BackupLocation: this.backupLocation(),
            LastFileNameToRestore: this.lastFileNameToRestore(),
            DataDirectory: dataDirectory,
            JournalsStoragePath: journalsStoragePath,
            IndexingStoragePath: indexingStoragePath,
            TempPath: tempPath,
            EncryptionKey: this.getEncryptionConfigSection().enabled() ? this.encryption.key() : null
        };
    }
}

export = databaseCreationModel;
