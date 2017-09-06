import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import databasesManager = require("common/shell/databasesManager");
import createDatabaseCommand = require("commands/resources/createDatabaseCommand");
import restoreDatabaseFromBackupCommand = require("commands/resources/restoreDatabaseFromBackupCommand");
import getClusterTopologyCommand = require("commands/database/cluster/getClusterTopologyCommand");
import generateSecretCommand = require("commands/database/secrets/generateSecretCommand");
import distributeSecretCommand = require("commands/database/secrets/distributeSecretCommand");
import clusterTopology = require("models/database/cluster/clusterTopology");
import clusterNode = require("models/database/cluster/clusterNode");
import copyToClipboard = require("common/copyToClipboard");
import databaseCreationModel = require("models/resources/creation/databaseCreationModel");
import eventsCollector = require("common/eventsCollector");
import fileDownloader = require("common/fileDownloader");
import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");

class createDatabase extends dialogViewModelBase {

    static readonly defaultSection = "Replication";

    spinners = {
        create: ko.observable<boolean>(false)
    };

    databaseModel = new databaseCreationModel();
    clusterNodes = [] as clusterNode[];
    
    encryptionSection: setupEncryptionKey;

    protected currentAdvancedSection = ko.observable<string>(createDatabase.defaultSection);

    showReplicationFactorWarning: KnockoutComputed<boolean>;
    enforceManualNodeSelection: KnockoutComputed<boolean>;
    disableReplicationFactorInput: KnockoutComputed<boolean>;
    selectionState: KnockoutComputed<checkbox>;
    canUseDynamicOption: KnockoutComputed<boolean>; 

    getDatabaseByName(name: string): database {
        return databasesManager.default.getDatabaseByName(name);
    }

    advancedVisibility = {
        encryption: ko.pureComputed(() => this.currentAdvancedSection() === "Encryption"),
        replication: ko.pureComputed(() => this.currentAdvancedSection() === "Replication"),
        path: ko.pureComputed(() => this.currentAdvancedSection() === "Path")
    };

    constructor(isFromBackup: boolean) {
        super();

        this.databaseModel.isFromBackup = isFromBackup;
        
        this.encryptionSection = new setupEncryptionKey(this.databaseModel.encryption.key, this.databaseModel.encryption.confirmation, this.databaseModel.name);

        this.bindToCurrentInstance("showAdvancedConfigurationFor", "toggleSelectAll");
    }

    activate() {
        //TODO: if !!this.licenseStatus() && this.licenseStatus().IsCommercial && this.licenseStatus().Attributes.periodicBackup !== "true" preselect periodic export
        //TODO: fetchClusterWideConfig
        //TODO: fetchCustomBundles

        const getTopologyTask = new getClusterTopologyCommand()
            .execute()
            .done(topology => {
                this.onTopologyLoaded(topology);
                this.initObservables();
            });

        const getEncryptionKeyTask = this.encryptionSection.generateEncryptionKey();

        return $.when<any>(getTopologyTask, getEncryptionKeyTask)
            .done(() => {
                // setup validation after we fetch and populate form with data
                this.databaseModel.setupValidation((name: string) => !this.getDatabaseByName(name), this.clusterNodes.length);
            });
    }

    compositionComplete() {
        super.compositionComplete();
        this.encryptionSection.syncQrCode();
        this.setupDisableReasons("#savingKeyData");

        this.databaseModel.encryption.key.subscribe(() => {
            this.encryptionSection.syncQrCode();
            // reset confirmation
            this.databaseModel.encryption.confirmation(false);
        });

        if (this.databaseModel.isFromBackup) {
            this.databaseModel.replication.replicationFactor(1);
        }
    }

    private onTopologyLoaded(topology: clusterTopology) {
        this.clusterNodes = topology.nodes();
        const defaultReplicationFactor = this.clusterNodes.length > 1 ? 2 : 1;
        this.databaseModel.replication.replicationFactor(defaultReplicationFactor);
    }

    protected initObservables() {

        this.canUseDynamicOption = ko.pureComputed(() => {
            const fromBackup = this.databaseModel.isFromBackup;
            const enforceManual = this.enforceManualNodeSelection();
            const replicationFactor = this.databaseModel.replication.replicationFactor();
            return !fromBackup && !enforceManual && replicationFactor !== 1;
        });

        // hide advanced if respononding bundle was unchecked
        this.databaseModel.configurationSections.forEach(section => {
            section.enabled.subscribe(enabled => {
                if (section.alwaysEnabled || enabled) {
                    this.currentAdvancedSection(section.name);
                } else if (!enabled && this.currentAdvancedSection() === section.name) {
                    this.currentAdvancedSection(createDatabase.defaultSection);
                }
            });
        });

        this.databaseModel.configurationSections.forEach(section => {
            if (!section.hasOwnProperty('validationGroup')) {
                section.validationGroup = undefined;
            }
        });

        const encryption = this.databaseModel.configurationSections.find(x => x.name === "Encryption");
        encryption.enabled.subscribe(encryptionEnabled => {
            if (encryptionEnabled) {
                this.databaseModel.replication.dynamicMode(false);
                this.databaseModel.replication.manualMode(true);
            }
        });

        this.showReplicationFactorWarning = ko.pureComputed(() => {
            const factor = this.databaseModel.replication.replicationFactor();
            return factor === 1;
        });

        this.enforceManualNodeSelection = ko.pureComputed(() => {
            const fromBackup = this.databaseModel.isFromBackup;
            const encryptionEnabled = this.databaseModel.getEncryptionConfigSection().enabled();

            return encryptionEnabled || fromBackup;
        });

        this.disableReplicationFactorInput = ko.pureComputed(() => {
            return this.databaseModel.replication.manualMode();
        });

        this.selectionState = ko.pureComputed<checkbox>(() => {
            const clusterNodes = this.clusterNodes;
            const selectedCount = this.databaseModel.replication.nodes().length;

            if (clusterNodes.length && selectedCount === clusterNodes.length)
                return checkbox.Checked;
            if (selectedCount > 0)
                return checkbox.SomeChecked;
            return checkbox.UnChecked;
        });
    }

    getAvailableSections() {
        return this.databaseModel.configurationSections;
    }

    createDatabase() {
        if (this.databaseModel.isFromBackup) {
            eventsCollector.default.reportEvent("database", "restore");
        } else {
            eventsCollector.default.reportEvent("database", "create");
        }

        const globalValid = this.isValid(this.databaseModel.globalValidationGroup);

        const sectionsValidityList = this.databaseModel.configurationSections.map(section => {
            if (section.enabled()) {
                return this.isValid(section.validationGroup);
            } else {
                return true;
            }
        });

        const allValid = globalValid && _.every(sectionsValidityList, x => !!x);

        if (allValid) {
            // disable validation for name as it might display error: database already exists
            // since we get async notifications during db creation
            this.databaseModel.name.extend({ validatable: false });
            
            if (this.databaseModel.isFromBackup) {
                this.createDatabaseFromBackup();
            } else {
                this.createDatabaseInternal();
            }
            return;
        }

        const firstInvalidSection = sectionsValidityList.indexOf(false);
        if (firstInvalidSection !== -1) {
            const sectionToShow = this.databaseModel.configurationSections[firstInvalidSection].name;
            this.showAdvancedConfigurationFor(sectionToShow);
        }
    }

    showAdvancedConfigurationFor(sectionName: string) {
        this.currentAdvancedSection(sectionName);

        const sectionConfiguration = this.databaseModel.configurationSections.find(x => x.name === sectionName);
        if (!sectionConfiguration.enabled()) {
            sectionConfiguration.enabled(true);
        }
    }

    private createDatabaseInternal(): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasePutResult> {
        this.spinners.create(true);

        const databaseDocument = this.databaseModel.toDto();
        const replicationFactor = this.databaseModel.replication.replicationFactor();

        databasesManager.default.activateAfterCreation(databaseDocument.DatabaseName);

        const encryptionTask = $.Deferred<void>();

        const encryptionSection = this.databaseModel.configurationSections.find(x => x.name === "Encryption");
        if (encryptionSection.enabled()) {
            const nodeTags = this.databaseModel.replication.nodes().map(x => x.tag());
            this.encryptionSection.configureEncryption(this.databaseModel.encryption.key(), nodeTags)
                .done(() => encryptionTask.resolve());
        } else {
            encryptionTask.resolve();
        }
        
        return encryptionTask
            .then(() => {
                return new createDatabaseCommand(databaseDocument, replicationFactor)
                    .execute()
                    .always(() => {
                        dialog.close(this);
                        this.spinners.create(false);
                    });
            })
            .fail(() => this.spinners.create(false));
    }

    private createDatabaseFromBackup(): JQueryPromise<operationIdDto> {
        this.spinners.create(true);

        const restoreDocument = this.databaseModel.toRestoreDocumentDto();

        return new restoreDatabaseFromBackupCommand(restoreDocument)
            .execute()
            .always(() => {
                dialog.close(this);
                this.spinners.create(false);
            });
    }

    toggleSelectAll() {
        const replicationConfig = this.databaseModel.replication;
        const selectedCount = replicationConfig.nodes().length;

        if (selectedCount > 0) {
            replicationConfig.nodes([]);
        } else {
            replicationConfig.nodes(this.clusterNodes.slice());
        }
    }

}

export = createDatabase;
