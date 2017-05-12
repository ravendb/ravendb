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

    static readonly defaultSection = "Replication";

    spinners = {
        create: ko.observable<boolean>(false)
    }

    databaseModel = new databaseCreationModel();
    clusterNodes = [] as clusterNode[];

    protected currentAdvancedSection = ko.observable<string>(createDatabase.defaultSection);

    showReplicationFactorWarning: KnockoutComputed<boolean>;

    enforceManualNodeSelection: KnockoutComputed<boolean>;

    indexesPathPlaceholder: KnockoutComputed<string>;

    getDatabaseByName(name: string): database {
        return databasesManager.default.getDatabaseByName(name);
    }

    advancedVisibility = {
        encryption: ko.pureComputed(() => this.currentAdvancedSection() === "Encryption"),
        replication: ko.pureComputed(() => this.currentAdvancedSection() === "Replication"),
        path: ko.pureComputed(() => this.currentAdvancedSection() === "Path")
    }

    constructor() {
        super();

        this.bindToCurrentInstance("showAdvancedConfigurationFor");
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

        return $.when<any>(getTopologyTask, getEncryptionKeyTask)
            .done(() => {
                // setup validation after we fetch and populate form with data
                this.databaseModel.setupValidation((name: string) => !this.getDatabaseByName(name), this.clusterNodes.length);
            });
    }

    private onTopologyLoaded(topology: clusterTopology) {
        this.clusterNodes = topology.nodes();
    }

    protected initObservables() {
        // hide advanced if respononding bundle was unchecked
        this.databaseModel.configurationSections.forEach(section => {
            section.enabled.subscribe(enabled => {
                if (!enabled && this.currentAdvancedSection() === section.name) {
                    this.currentAdvancedSection(createDatabase.defaultSection);
                }
                if (enabled) {
                    this.currentAdvancedSection(section.name);
                }
            });
        });

        this.indexesPathPlaceholder = ko.pureComputed(() => {
            const name = this.databaseModel.name();
            return `~/${name || "{Database Name}"}/Indexes/`;
        });

        this.databaseModel.configurationSections.forEach(section => {
            if (!section.hasOwnProperty('validationGroup')) {
                section.validationGroup = undefined;
            }
        });

        this.showReplicationFactorWarning = ko.pureComputed(() => {
            const factor = this.databaseModel.replication.replicationFactor();
            return factor === 1;
        });

        this.enforceManualNodeSelection = ko.pureComputed(() => {
            return this.databaseModel.getEncryptionConfigSection().enabled();
        });
    }

    getAvailableSections() {
        return this.databaseModel.configurationSections;
    }

    createDatabase() {
        eventsCollector.default.reportEvent('database', 'create');

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
            this.createDatabaseInternal();
        } else {
            const firstInvalidSection = sectionsValidityList.indexOf(false);
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

    protected generateEncryptionKey(): JQueryPromise<string> {
        return new generateSecretCommand()
            .execute()
            .done(secret => {
                this.databaseModel.encryption.key(secret);
            });
    }

    private createDatabaseInternal(): JQueryPromise<void> {
        this.spinners.create(true);

        const databaseDocument = this.databaseModel.toDto();
        const replicationFactor = this.databaseModel.replication.replicationFactor();

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
        const encryptionSection = this.databaseModel.configurationSections.find(x => x.name === "Encryption");
        if (encryptionSection.enabled()) {
            return new putSecretCommand(databaseName, encryptionKey, false)
                .execute();
        } else {
            return $.Deferred<void>().resolve();
        }
    }

}

export = createDatabase;
