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
import copyToClipboard = require("common/copyToClipboard");

import databaseCreationModel = require("models/resources/creation/databaseCreationModel");
import eventsCollector = require("common/eventsCollector");
import fileDownloader = require("common/fileDownloader");

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
    disableReplicationFactorInput: KnockoutComputed<boolean>;
    indexesPathPlaceholder: KnockoutComputed<string>;
    selectionState: KnockoutComputed<checkbox>;

    getDatabaseByName(name: string): database {
        return databasesManager.default.getDatabaseByName(name);
    }

    advancedVisibility = {
        encryption: ko.pureComputed(() => this.currentAdvancedSection() === "Encryption"),
        replication: ko.pureComputed(() => this.currentAdvancedSection() === "Replication"),
        path: ko.pureComputed(() => this.currentAdvancedSection() === "Path")
    }

    // currently displayed QR Code
    private qrCode: any; 

    constructor() {
        super();

        this.bindToCurrentInstance("showAdvancedConfigurationFor", "toggleSelectAll", "copyEncryptionKeyToClipboard");
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

    compositionComplete() {
        super.compositionComplete();

        this.syncQrCode();
        this.databaseModel.encryption.key.subscribe(() => this.syncQrCode());
    }

    private onTopologyLoaded(topology: clusterTopology) {
        this.clusterNodes = topology.nodes();
        const defaultReplicationFactor = this.clusterNodes.length > 1 ? 2 : 1;
        this.databaseModel.replication.replicationFactor(defaultReplicationFactor);
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

    private syncQrCode() {
        const key = this.databaseModel.encryption.key();
        const qrContainer = document.getElementById("encryption_qrcode");

        const isKeyValid = this.databaseModel.encryption.key.isValid();

        if (isKeyValid) {
            if (!this.qrCode) {
                this.qrCode = new QRCode(qrContainer, {
                    text: key,
                    width: 256,
                    height: 256,
                    colorDark: "#000000",
                    colorLight: "#ffffff",
                    correctLevel: QRCode.CorrectLevel.Q
                });
            } else {
                this.qrCode.clear();
                this.qrCode.makeCode(key);
            }
        } else {
            if (this.qrCode) {
                this.qrCode.clear();
            }
        }
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

    copyEncryptionKeyToClipboard() {
        const key = this.databaseModel.encryption.key();
        const container = document.getElementById("newDatabase");
        copyToClipboard.copy(key, "Encryption key was copied to clipboard", container);
    }

    downloadEncryptionKey() {
        //TODO: work on text


        const encryptionKey = this.databaseModel.encryption.key();
        const text = `Your encryption key: ${encryptionKey}`;
        fileDownloader.downloadAsTxt(text, "key.txt");
    }

    printEncryptionKey() {
        const printWindow = window.open();

        const encryptionKey = this.databaseModel.encryption.key();
        const text = `Your encryption key: ${encryptionKey}`;

        const qrCodeHtml = document.getElementById("encryption_qrcode").innerHTML;

        //TODO: work on wording here

        let html = "<html>";
        html += text;
        html += "<br />";
        html += qrCodeHtml;
        html += "</html>";

        try {
            printWindow.document.write(html);
            printWindow.document.close();

            printWindow.focus();
            printWindow.print();
        } finally {
            printWindow.close();
        }
    }

}

export = createDatabase;
