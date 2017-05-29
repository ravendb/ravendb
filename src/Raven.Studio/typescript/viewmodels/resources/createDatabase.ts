import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import databasesManager = require("common/shell/databasesManager");
import createDatabaseCommand = require("commands/resources/createDatabaseCommand");
import getClusterTopologyCommand = require("commands/database/cluster/getClusterTopologyCommand");
import generateSecretCommand = require("commands/database/secrets/generateSecretCommand");
import distributeSecretCommand = require("commands/database/secrets/distributeSecretCommand");
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

        const getTopologyTask = new getClusterTopologyCommand()
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
        this.databaseModel.encryption.key.subscribe(() => {
            this.syncQrCode();
            // reset confirmation
            this.databaseModel.encryption.confirmation(false);
        });
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
                if (section.alwaysEnabled || enabled) {
                    this.currentAdvancedSection(section.name);
                } else if (!enabled && this.currentAdvancedSection() === section.name) {
                    this.currentAdvancedSection(createDatabase.defaultSection);
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
            if (firstInvalidSection !== -1) {
                const sectionToShow = this.databaseModel.configurationSections[firstInvalidSection].name;
                this.showAdvancedConfigurationFor(sectionToShow);
            }
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

    private createDatabaseInternal(): JQueryPromise<Raven.Server.Web.System.DatabasePutResult> {
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
            const nodeTags = this.databaseModel.replication.nodes().map(x => x.tag());
            return new distributeSecretCommand(databaseName, encryptionKey, nodeTags)
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
        const html = `
            <html>
                <h4>${text}</h4>
                <br />
                <div id="encryption_qrcode">
                    <img class="qr_logo" src="data:image/png;base64,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">
                    ${qrCodeHtml}
                </div>
                <style>
                    body {
                        text-align: center;
                        font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
                    }
                    #encryption_qrcode {
                        position: relative;
                        display: inline-block;
                    }

                    h4 {
                        font-weight: normal;
                    }
                    
                    .qr_logo {
                        position: absolute;
                        left: 50%;
                        top: 50%;
                        -moz-transform: translateX(-50%) translateY(-50%);
                        -webkit-transform: translateX(-50%) translateY(-50%);
                        -o-transform: translateX(-50%) translateY(-50%);
                        -ms-transform: translateX(-50%) translateY(-50%);
                        transform: translateX(-50%) translateY(-50%);
                    }
                </style>
            </html>
        `;

        printWindow.document.write(html);
        printWindow.document.close();

        printWindow.focus();
        setTimeout(() => {
            printWindow.print();
            printWindow.close();
        }, 50);
    }
}

export = createDatabase;
