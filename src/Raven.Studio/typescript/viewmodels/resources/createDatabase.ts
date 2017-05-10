import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import databasesManager = require("common/shell/databasesManager");
import createDatabaseCommand = require("commands/resources/createDatabaseCommand");
import getClusterTopologyCommand = require("commands/database/cluster/getClusterTopologyCommand");
import generateSecretCommand = require("commands/database/secrets/generateSecretCommand");
import putSecretCommand = require("commands/database/secrets/putSecretCommand");
import clusterTopology = require("models/database/cluster/clusterTopology");
import clusterNode = require("models/database/cluster/clusterNode");

import databaseCreationModel = require("models/resources/creation/databaseCreationModel");
import eventsCollector = require("common/eventsCollector");

class createDatabase extends dialogViewModelBase {

    readonly databaseBundles: Array<availableBundle> = [
        {
            displayName: "Replication",
            name: "Replication",
            hasAdvancedConfiguration: true
        },
        {
            displayName: "Encryption",
            name: "Encryption",
            hasAdvancedConfiguration: true
        }
    ];

    spinners = {
        create: ko.observable<boolean>(false)
    }

    bundlesEnabled = {
        encryption: this.isBundleActiveComputed("Encryption"),
        replication: this.isBundleActiveComputed("Replication")
    }

    databaseModel = new databaseCreationModel();
    clusterNodes = [] as clusterNode[];

    protected advancedBundleConfigurationVisible = ko.observable<string>();
    advancedConfigurationVisible = ko.observable<boolean>(false);
    showWideDialog: KnockoutComputed<boolean>;
    showReplicationFactorWarning: KnockoutComputed<boolean>;

    indexesPathPlaceholder: KnockoutComputed<string>;

    getDatabaseByName(name: string): database {
        return databasesManager.default.getDatabaseByName(name);
    }

    advancedVisibility = {
        encryption: ko.pureComputed(() => this.advancedBundleConfigurationVisible() === "Encryption"),
        replication: ko.pureComputed(() => this.advancedBundleConfigurationVisible() === "Replication")
    }

    activate() {
        //TODO: if !!this.licenseStatus() && this.licenseStatus().IsCommercial && this.licenseStatus().Attributes.periodicBackup !== "true" preselect periodic export
        //TODO: fetchClusterWideConfig
        //TODO: fetchCustomBundles

        const getTopologyTask = new getClusterTopologyCommand(window.location.host)
            .execute()
            .done(topology => {
                this.onTopologyLoaded(topology);
                this.initObservables();
            });

        const getEncryptionKeyTask = this.generateEncryptionKey();

        return $.when<any>(getTopologyTask, getEncryptionKeyTask);
    }

    private onTopologyLoaded(topology: clusterTopology) {
        this.clusterNodes = topology.nodes();
    }

    protected initObservables() {
        this.showWideDialog = ko.pureComputed(() => {
            const hasAdvancedOpened = this.advancedConfigurationVisible();
            const hasAdvancedBundleOpened = !!this.advancedBundleConfigurationVisible();

            return hasAdvancedBundleOpened || hasAdvancedOpened;
        });

        this.databaseModel.selectedBundles.subscribe((changes: Array<KnockoutArrayChange<string>>) => {
            // hide advanced if respononding bundle was unchecked
            if (!this.advancedBundleConfigurationVisible()) {
                return;
            }
            changes.forEach(change => {
                if (change.status === "deleted" && change.value === this.advancedBundleConfigurationVisible()) {
                    this.advancedBundleConfigurationVisible(null);
                }
            });
        }, null, "arrayChange");

        this.databaseModel.setupValidation((name: string) => !this.getDatabaseByName(name), this.clusterNodes.length);

        this.indexesPathPlaceholder = ko.pureComputed(() => {
            const name = this.databaseModel.name();
            return `~/${name || "{Database Name}"}/Indexes/`;
        });

        this.databaseBundles.forEach(bundle => {
            if (!bundle.hasOwnProperty('validationGroup')) {
                bundle.validationGroup = undefined;
            }
        });

        const encryptionConfig = this.databaseBundles.find(x => x.name === "Encryption");
        encryptionConfig.validationGroup = this.databaseModel.encryptionValidationGroup;

        const replicationConfig = this.databaseBundles.find(x => x.name === "Replication");
        replicationConfig.validationGroup = this.databaseModel.replicationValidationGroup;

        this.showReplicationFactorWarning = ko.pureComputed(() => {
            const factor = this.databaseModel.replicationFactor();
            return factor === 1;
        });
    }

    getAvailableBundles() {
        //TODO: concat with custom bundles 
        return this.databaseBundles;
    }

    createDatabase() {
        eventsCollector.default.reportEvent('database', 'create');

        const globalValid = this.isValid(this.databaseModel.globalValidationGroup);
        const advancedValid = this.isValid(this.databaseModel.advancedValidationGroup);
        const encryptionValid = !this.bundlesEnabled.encryption() || this.isValid(this.databaseModel.encryptionValidationGroup);
        const replicationValid = !this.bundlesEnabled.replication() || this.isValid(this.databaseModel.replicationValidationGroup);

        const allValid = globalValid && advancedValid && encryptionValid && replicationValid;

        if (allValid) {
            this.createDatabaseInternal();
        } else {

            if (!replicationValid) {
                if (!this.advancedVisibility.replication()) {
                    this.showAdvancedConfigurationFor("Replication");
                }
            } else if (!encryptionValid) {
                if (!this.advancedVisibility.encryption()) {
                    this.showAdvancedConfigurationFor("Encryption");
                }
            } else {
                if (!this.advancedConfigurationVisible()) {
                    this.showAdvancedConfiguration();
                }
            }
        }
    }

    showAdvancedConfiguration() {
        if (this.advancedConfigurationVisible()) {
            this.advancedConfigurationVisible(false);
            return;
        }

        this.advancedBundleConfigurationVisible(null);
        this.advancedConfigurationVisible(true);
    }

    showAdvancedConfigurationFor(bundleName: string) {
        if (this.advancedBundleConfigurationVisible() === bundleName) {
            this.advancedBundleConfigurationVisible(null);
            return;
        }

        if (!_.includes(this.databaseModel.selectedBundles(), bundleName)) {
            this.databaseModel.selectedBundles.push(bundleName);
        }
        this.advancedConfigurationVisible(false);
        this.advancedBundleConfigurationVisible(bundleName);
    }

    protected generateEncryptionKey(): JQueryPromise<string> {
        return new generateSecretCommand()
            .execute()
            .done(secret => {
                this.databaseModel.encryption.key(secret);
            });
    }

    isBundleActive(name: string): boolean {
        //TODO: implement me!
        return true;
    }

    private createDatabaseInternal(): JQueryPromise<void> {
        this.spinners.create(true);

        const databaseDocument = this.databaseModel.toDto();
        const replicationFactor = this.databaseModel.replicationFactor();

        databasesManager.default.activateAfterCreation(databaseDocument.DatabaseName);

        return this.configureEncryptionIfNeeded(databaseDocument.DatabaseName, this.databaseModel.encryption.key())
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

    private configureEncryptionIfNeeded(databaseName: string, encryptionKey: string): JQueryPromise<void> {
        if (this.bundlesEnabled.encryption()) {
            return new putSecretCommand(databaseName, encryptionKey, false)
                .execute();
        } else {
            return $.Deferred<void>().resolve();
        }
    }

    private isBundleActiveComputed(bundleName: string) {
        return ko.pureComputed(() => _.includes(this.databaseModel.selectedBundles(), bundleName));
    }

}

export = createDatabase;
