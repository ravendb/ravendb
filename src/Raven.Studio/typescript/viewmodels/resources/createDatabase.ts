import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import databasesManager = require("common/shell/databasesManager");
import createDatabaseCommand = require("commands/resources/createDatabaseCommand");
import restoreDatabaseFromBackupCommand = require("commands/resources/restoreDatabaseFromBackupCommand");
import migrateLegacyDatabaseFromDatafilesCommand = require("commands/resources/migrateLegacyDatabaseFromDatafilesCommand");
import getClusterTopologyCommand = require("commands/database/cluster/getClusterTopologyCommand");
import getDatabaseLocationCommand = require("commands/resources/getDatabaseLocationCommand");
import getFolderPathOptionsCommand = require("commands/resources/getFolderPathOptionsCommand");
import clusterTopology = require("models/database/cluster/clusterTopology");
import clusterNode = require("models/database/cluster/clusterNode");
import databaseCreationModel = require("models/resources/creation/databaseCreationModel");
import eventsCollector = require("common/eventsCollector");
import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import popoverUtils = require("common/popoverUtils");
import notificationCenter = require("common/notifications/notificationCenter");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import viewHelpers = require("common/helpers/view/viewHelpers");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import lastUsedAutocomplete = require("common/storage/lastUsedAutocomplete");
import viewModelBase = require("viewmodels/viewModelBase");
import studioSettings = require("common/settings/studioSettings");
import licenseModel = require("models/auth/licenseModel");
import generalUtils = require("common/generalUtils");

class createDatabase extends dialogViewModelBase {
    
    static readonly legacyKeySizes = [128, 192, 256];
    static readonly legacyEncryptionAlgorithms = ['DES', 'RC2', 'Rijndael', 'Triple DES'] as legacyEncryptionAlgorithms[];

    static readonly defaultSection = "replication";
    
    clientVersion = viewModelBase.clientVersion;

    spinners = {
        create: ko.observable<boolean>(false),
        databaseLocationInfoLoading: ko.observable<boolean>(false)
    };

    databaseModel: databaseCreationModel;
    clusterNodes = [] as clusterNode[];
    
    encryptionSection: setupEncryptionKey;
    usingHttps = location.protocol === "https:"; 
    operationNotSupported: boolean;
    
    protected currentAdvancedSection = ko.observable<availableConfigurationSectionId>();

    showDynamicDatabaseDistributionWarning: KnockoutComputed<boolean>;
    showReplicationFactorWarning: KnockoutComputed<boolean>;
    showNumberOfShardsWarning: KnockoutComputed<boolean>;
    enforceManualNodeSelection: KnockoutComputed<boolean>;
    disableReplicationFactorInput: KnockoutComputed<boolean>;
    selectionState: KnockoutComputed<checkbox>;
    canUseDynamicOption: KnockoutComputed<boolean>; 
    defaultReplicationFactor = ko.observable<number>();

    databaseLocationInfo = ko.observableArray<Raven.Server.Web.Studio.SingleNodeDataDirectoryResult>([]);
    databaseLocationInfoToDisplay: KnockoutComputed<Raven.Server.Web.Studio.SingleNodeDataDirectoryResult[]>;
    databaseLocationInfoMessage: KnockoutComputed<string>;

    recentPathsAutocomplete: lastUsedAutocomplete;
    dataExporterAutocomplete: lastUsedAutocomplete;
    
    dataExporterDirectoryPathOptions = ko.observableArray<string>([]);
    backupFolderPathOptions = ko.observableArray<string>([]);
    sourceJournalsPathOptions = ko.observableArray<string>([]);
    
    allAutoCompleteOptions: KnockoutComputed<{ path: string, isRecent: boolean }[]>;
    allDataExporterAutoCompleteOptions: KnockoutComputed<{ path: string, isRecent: boolean }[]>;

    legacyMigrationDataDirectoryPathOptions = ko.observableArray<string>([]);
    
    getDatabaseByName(name: string): database {
        return databasesManager.default.getDatabaseByName(name);
    }

    advancedVisibility = {
        legacyMigration: ko.pureComputed(() => this.currentAdvancedSection() === "legacyMigration"), 
        restore: ko.pureComputed(() => this.currentAdvancedSection() === "restore"),
        encryption: ko.pureComputed(() => this.currentAdvancedSection() === "encryption"),
        replication: ko.pureComputed(() => this.currentAdvancedSection() === "replication"),
        sharding: ko.pureComputed(() => this.currentAdvancedSection() === "sharding"),
        path: ko.pureComputed(() => this.currentAdvancedSection() === "path")
    };

    constructor(mode: dbCreationMode) {
        super();

        this.operationNotSupported = mode === "legacyMigration" && clusterTopologyManager.default.nodeInfo().OsInfo.Type !== "Windows";
        
        const canCreateEncryptedDatabases = ko.pureComputed(() => this.usingHttps && licenseModel.licenseStatus() && licenseModel.licenseStatus().HasEncryption);
        this.databaseModel = new databaseCreationModel(mode, canCreateEncryptedDatabases);
        this.recentPathsAutocomplete = new lastUsedAutocomplete("createDatabasePath", this.databaseModel.path.dataPath);
        this.dataExporterAutocomplete = new lastUsedAutocomplete("dataExporterPath", this.databaseModel.legacyMigration.dataExporterFullPath);
        
        switch (mode) {
            case "newDatabase": 
                this.currentAdvancedSection(createDatabase.defaultSection);
                break;
            case "restore":
                this.currentAdvancedSection("restore");
                break;
            case "legacyMigration":
                this.currentAdvancedSection("legacyMigration");
                break;
        }
        
        this.encryptionSection = setupEncryptionKey.forDatabase(this.databaseModel.encryption.key, this.databaseModel.encryption.confirmation, this.databaseModel.name);

        this.bindToCurrentInstance("showAdvancedConfigurationFor", "toggleSelectAll");
    }

    activate() {
        const getStudioSettingsTask = studioSettings.default.globalSettings()
            .then(settings => {
                this.defaultReplicationFactor(settings.replicationFactor.getValue());
            });
        
        const getTopologyTask = getStudioSettingsTask.then(() => {
            return new getClusterTopologyCommand()
                .execute()
                .done(topology => {
                    this.setDefaultReplicationFactor(topology);
                    this.initObservables();
                });
        });

        const getEncryptionKeyTask = this.encryptionSection.generateEncryptionKey();

        const dataPath = this.databaseModel.path.dataPath();
        this.updateDatabaseLocationInfo(this.databaseModel.name(), dataPath);
        
        this.updateBackupDirectoryPathOptions();
        
        if (this.databaseModel.creationMode === "legacyMigration") {
            this.updateLegacyMigrationDataDirectoryPathOptions(this.databaseModel.legacyMigration.dataDirectory());
            this.updateLegacyDataExporterPath(this.databaseModel.legacyMigration.dataExporterFullPath());
            this.updateSourceJournalsPathOptions(this.databaseModel.legacyMigration.journalsPath());
        }

        return $.when<any>(getTopologyTask, getEncryptionKeyTask, getStudioSettingsTask)
            .done(() => {
                // setup validation after we fetch and populate form with data
                this.databaseModel.setupValidation((name: string) => !this.getDatabaseByName(name), this.clusterNodes.length);
            });
    }

    compositionComplete() {
        super.compositionComplete();
        this.encryptionSection.syncQrCode();
        this.setupDisableReasons("#savingKeyData");
        
        popoverUtils.longWithHover($(".resource-type-label small"),
            {
                content: 'RavenFS files will be saved as documents with attachments in <strong>@files</strong> collection.'
            });

        popoverUtils.longWithHover($(".data-exporter-label small"),
            {
                content: '<strong>Raven.StorageExporter.exe</strong> can be found in <strong>tools</strong><br /> package (for version v3.x) on <a target="_blank" href="http://ravendb.net/downloads">ravendb.net</a> website'
            });
        
        popoverUtils.longWithHover($(".data-directory-label small"), 
            {
                content: 'Absolute path to data directory. <br/> This folder should contain file <strong>Data</strong>, <strong>Data.ravenfs</strong> or <strong>Raven.voron</strong>.'
            });

        this.databaseModel.encryption.key.subscribe(() => {
            this.encryptionSection.syncQrCode();
            // reset confirmation
            this.databaseModel.encryption.confirmation(false);
        });

        if (this.databaseModel.isFromBackupOrFromOfflineMigration) {
            this.databaseModel.replication.replicationFactor(1);
        }
        
        // if we have single node - select it to make things easier
        if (this.clusterNodes.length === 1) {
            this.databaseModel.replication.nodes([this.clusterNodes[0]]);
        }
        
        this.databaseModel.restore.selectedRestorePoint.subscribe(restorePoint => {
            this.encryptionSection.canProvideOwnKey(!restorePoint || !restorePoint.isEncrypted || !restorePoint.isSnapshotRestore);
        });
        
        this.databaseModel.restore.backupEncryptionKey.subscribe(key => {
            const restorePoint = this.databaseModel.restore.selectedRestorePoint();
            if (restorePoint && restorePoint.isEncrypted && restorePoint.isSnapshotRestore) {
                // update database encryption key to match backup encryption key
                this.databaseModel.encryption.key(key);
            }
        });
    }

    private setDefaultReplicationFactor(topology: clusterTopology) {
        this.clusterNodes = topology.nodes();
        
        const defaultReplicationFactor = this.defaultReplicationFactor() || this.clusterNodes.length;
        this.databaseModel.replication.replicationFactor(defaultReplicationFactor);
    }

    protected initObservables() {
        this.canUseDynamicOption = ko.pureComputed(() => {
            const fromBackup = this.databaseModel.isFromBackupOrFromOfflineMigration;
            const enforceManual = this.enforceManualNodeSelection();
            const replicationFactor = this.databaseModel.replication.replicationFactor();
            return !fromBackup && !enforceManual && replicationFactor !== 1;
        });

        // hide advanced if corresponding bundle was unchecked
        this.databaseModel.configurationSections.forEach(section => {
            section.enabled.subscribe(enabled => {
                if (!this.databaseModel.lockActiveTab()) {
                    if (section.alwaysEnabled || enabled) {
                        this.currentAdvancedSection(section.id);
                    } else if (!enabled && this.currentAdvancedSection() === section.id) {
                        this.currentAdvancedSection(createDatabase.defaultSection);
                    }
                }
            });
        });

        this.databaseModel.configurationSections.forEach(section => {
            if (!section.hasOwnProperty('validationGroup')) {
                section.validationGroup = undefined;
            }
        });

        const encryption = this.databaseModel.configurationSections.find(x => x.id === "encryption");
        encryption.enabled.subscribe(encryptionEnabled => {
            const creationMode = this.databaseModel.creationMode;
            const canUseManualMode = creationMode === "newDatabase";
            if (encryptionEnabled && canUseManualMode) {
                this.databaseModel.replication.dynamicMode(false);
                this.databaseModel.replication.manualMode(true);
            }
        });

        this.showDynamicDatabaseDistributionWarning = ko.pureComputed(() => {
            const hasDynamicDatabaseDistribution = licenseModel.licenseStatus().HasDynamicNodesDistribution;
            if (!hasDynamicDatabaseDistribution) {
                this.databaseModel.replication.dynamicMode(false);
            }
            return !hasDynamicDatabaseDistribution;
        });

        this.showReplicationFactorWarning = ko.pureComputed(() => {
            const factor = this.databaseModel.replication.replicationFactor();
            return factor === 1;
        });

        this.showNumberOfShardsWarning = ko.pureComputed(() => {
            const shards = this.databaseModel.sharding.numberOfShards();
            return shards === 0;
        });

        this.enforceManualNodeSelection = ko.pureComputed(() => {
            const fromBackup = this.databaseModel.creationMode !== "newDatabase";
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
                return "checked";
            if (selectedCount > 0)
                return "some_checked";
            return "unchecked";
        });

        this.databaseModel.path.dataPath.throttle(300).subscribe(newPath => {
            if (this.databaseModel.path.dataPath.isValid()) {
                this.updateDatabaseLocationInfo(this.databaseModel.name(), newPath);
            } else {
                this.databaseLocationInfo([]);
                this.spinners.databaseLocationInfoLoading(false);
            }
        });

        this.databaseModel.name.throttle(300).subscribe(newDatabaseName => {
            const dataPath = this.databaseModel.path.dataPath();
            if (dataPath) {
                return;
            }

            if (this.databaseModel.path.dataPath.isValid()) {
                this.updateDatabaseLocationInfo(newDatabaseName, dataPath);
            } else {
                this.databaseLocationInfo([]);
                this.spinners.databaseLocationInfoLoading(false);
            }
        });

        this.databaseModel.restore.azureCredentials().onCredentialsChange(() => {
            this.updateBackupDirectoryPathOptions();
            this.databaseModel.fetchRestorePoints(true);
        });
        this.databaseModel.restore.amazonS3Credentials().onCredentialsChange(() => {
            this.updateBackupDirectoryPathOptions();
            this.databaseModel.fetchRestorePoints(true);
        });
        this.databaseModel.restore.googleCloudCredentials().onCredentialsChange(() => {
            this.updateBackupDirectoryPathOptions();
            this.databaseModel.fetchRestorePoints(true);
        });
        this.databaseModel.restore.localServerCredentials().onCredentialsChange(() => {
            this.updateBackupDirectoryPathOptions();
            this.databaseModel.fetchRestorePoints(true);
        });
        
        this.databaseModel.restoreSourceObject.subscribe(() => {
            this.updateBackupDirectoryPathOptions();
        });
        
        this.databaseModel.legacyMigration.dataDirectory.throttle(300).subscribe(newPath => {
            this.updateLegacyMigrationDataDirectoryPathOptions(newPath);
        });
        this.databaseModel.legacyMigration.dataExporterFullPath.throttle(300).subscribe(newPath => {
           this.updateLegacyDataExporterPath(newPath); 
        });
        this.databaseModel.legacyMigration.journalsPath.throttle(300).subscribe(newPath => {
            this.updateSourceJournalsPathOptions(newPath);
        });

        this.databaseLocationInfoToDisplay = ko.pureComputed(() => {
            const databaseLocationInfo = this.databaseLocationInfo();
            const selectedNodes = this.databaseModel.replication.nodes().map(x => x.tag());
            const replicationFactor = this.databaseModel.replication.replicationFactor();
            if (this.databaseModel.replication.manualMode()) {
                if (selectedNodes.length === 0) {
                    return [];
                }
            } else if (replicationFactor === 0) {
                return [];
            }

            if (replicationFactor > 0 && selectedNodes.length === 0) {
                // on first load the replication factor is set but the selected nodes aren't
                // we cannot set it in CompositionComplete because the default replication factor might be smaller than the number of nodes
                return databaseLocationInfo;
            }

            return databaseLocationInfo.filter(x => selectedNodes.indexOf(x.NodeTag) > -1);
        });

        this.databaseLocationInfoMessage = ko.pureComputed(() => {
            const message = "The data files will be created in";
            const selectedNodes = this.databaseModel.replication.nodes().map(x => x.tag());
            if (this.databaseModel.replication.manualMode()) {
                if (selectedNodes.length === 0) {
                    return null;
                }

                return `${message}:`;
            }

            const numberOfClusterNodes = this.clusterNodes.length;
            const replicationFactor = Math.min(this.databaseModel.replication.replicationFactor(), numberOfClusterNodes);
            if (replicationFactor === 0) {
                return null;
            }

            if (replicationFactor === 1 ||
                replicationFactor === numberOfClusterNodes) {
                return `${message}:`;
            }

            return `${message} ${replicationFactor} of the ${numberOfClusterNodes} nodes:`;
        });

        this.allAutoCompleteOptions = ko.pureComputed(() => {
            const result: { path: string, isRecent: boolean }[] = [];

            const autoComplete = this.recentPathsAutocomplete.createCompleter();
            autoComplete().forEach(p => {
                result.push({ path: p, isRecent: true });
            });

            const pathOptions = this.backupFolderPathOptions();
            pathOptions.forEach(p => {
                result.push({ path: p, isRecent: false });
            });

            return result;
        });

        this.allDataExporterAutoCompleteOptions = ko.pureComputed(() => {
            const result: { path: string, isRecent: boolean }[] = [];

            const autoComplete = this.dataExporterAutocomplete.createCompleter();
            autoComplete().forEach(p => {
                result.push({ path: p, isRecent: true });
            });

            const pathOptions = this.dataExporterDirectoryPathOptions();
            pathOptions.forEach(p => {
                result.push({ path: p, isRecent: false });
            });

            return result;
        });
    }

    updateDatabaseLocationInfo(name: string, path: string) {
        const task = new getDatabaseLocationCommand(name, path)
            .execute()
            .done((result: Raven.Server.Web.Studio.DataDirectoryResult) => {
                if (this.databaseModel.name() !== name ||
                    this.databaseModel.path.dataPath() !== path) {
                    // the path and name were changed
                    return;
                }

                this.databaseLocationInfo(result.List);
            });
        
        generalUtils.delayedSpinner(this.spinners.databaseLocationInfoLoading, task);
    }  

    updateBackupDirectoryPathOptions() {
        this.databaseModel.restoreSourceObject().getFolderPathOptions()
            .done((optionsList: string[]) => this.backupFolderPathOptions(optionsList));
    }

    private updateLocalFolderPath(path: string, backupFolder = false): JQueryPromise<Raven.Server.Web.Studio.FolderPathOptions> {
        return getFolderPathOptionsCommand.forServerLocal(path, backupFolder)
            .execute();
    }
    
    updateLegacyMigrationDataDirectoryPathOptions(path: string) {
        this.updateLocalFolderPath(path)
            .done(result => this.legacyMigrationDataDirectoryPathOptions(result.List));
    }
    
    updateLegacyDataExporterPath(path: string) {
        this.updateLocalFolderPath(path)
            .done(result => this.dataExporterDirectoryPathOptions(result.List));
    }

    updateSourceJournalsPathOptions(path: string) {
        this.updateLocalFolderPath(path)
            .done(result => this.sourceJournalsPathOptions(result.List));
    }

    getAvailableSections() {
        let sections = this.databaseModel.configurationSections;

        const restoreSection = sections.find(x => x.id === "restore");
        const legacyMigrationSection = sections.find(x => x.id === "legacyMigration");
        
        switch (this.databaseModel.creationMode) {
            case "newDatabase":
                return _.without(sections, restoreSection, legacyMigrationSection);
            case "restore":
                return _.without(sections, legacyMigrationSection);
            case "legacyMigration":
                return _.without(sections, restoreSection);
        }
    }
    
    createDatabase() {
        viewHelpers.asyncValidationCompleted(this.databaseModel.globalValidationGroup, () => {

            eventsCollector.default.reportEvent("database", this.databaseModel.creationMode);

            const availableSections = this.getAvailableSections();

            const sectionsValidityList = availableSections.map(section => {
                if (section.enabled()) {
                    return this.isValid(section.validationGroup);
                } else {
                    return true;
                }
            });

            const globalValid = this.isValid(this.databaseModel.globalValidationGroup);
            const allValid = globalValid && _.every(sectionsValidityList, x => !!x);

            if (allValid) {
                // disable validation for name as it might display error: database already exists
                // since we get async notifications during db creation
                this.databaseModel.name.extend({validatable: false});
                
                this.recentPathsAutocomplete.recordUsage();
                this.dataExporterAutocomplete.recordUsage();

                switch (this.databaseModel.creationMode) {
                    case "restore":
                        this.createDatabaseFromBackup();
                        break;
                    case "newDatabase":
                        this.createDatabaseInternal();
                        break;
                    case "legacyMigration":
                        this.createDatabaseFromLegacyDatafiles();
                        break;
                }

                return;
            }

            const firstInvalidSection = sectionsValidityList.indexOf(false);
            if (firstInvalidSection !== -1) {
                const sectionToShow = availableSections[firstInvalidSection].id;
                this.showAdvancedConfigurationFor(sectionToShow);
            }
        });
    }

    showAdvancedConfigurationFor(sectionName: availableConfigurationSectionId) {
        this.currentAdvancedSection(sectionName);

        const sectionConfiguration = this.databaseModel.configurationSections.find(x => x.id === sectionName);
        if (!sectionConfiguration.enabled()) {
            sectionConfiguration.enabled(true);
        }
    }

    private createDatabaseInternal(shouldActive: boolean = true): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasePutResult> {
        this.spinners.create(true);

        const databaseDocument = this.databaseModel.toDto();
        const replicationFactor = this.databaseModel.replication.replicationFactor();

        if (shouldActive) {
            databasesManager.default.activateAfterCreation(databaseDocument.DatabaseName);    
        }

        const encryptionTask = $.Deferred<void>();

        const encryptionSection = this.databaseModel.configurationSections.find(x => x.id === "encryption");
        if (encryptionSection.enabled()) {
            const nodeTags = this.databaseModel.replication.nodes().map(x => x.tag());
            this.encryptionSection.configureEncryption(this.databaseModel.encryption.key(), nodeTags)
                .done(() => encryptionTask.resolve())
                .fail(() => this.spinners.create(false));                
        } else {
            encryptionTask.resolve();
        }
        
        return encryptionTask
            .then(() => {
                return new createDatabaseCommand(databaseDocument, replicationFactor)
                    .execute()
                    .done(result => {
                        // Notify the calling databases view whether this databases is sharded
                        // Can't rely on ws - see issue https://issues.hibernatingrhinos.com/issue/RavenDB-16177 // TODO
                        dialog.close(this, result.ShardsDefined);
                        this.spinners.create(false);
                    })
                    .fail(() => {
                        dialog.close(this);
                        this.spinners.create(false);
                    });
            });
    }

    private createDatabaseFromLegacyDatafiles(): JQueryPromise<operationIdDto> {  

        const restoreDocument = this.databaseModel.toOfflineMigrationDto();
        return new migrateLegacyDatabaseFromDatafilesCommand(restoreDocument)
            .execute()
            .done((operationIdDto: operationIdDto) => {
                const operationId = operationIdDto.OperationId;
                notificationCenter.instance.openDetailsForOperationById(null, operationId);
                dialog.close(this);
            })
            .always(() => {
                this.spinners.create(false);
            });
    }
    
    private createDatabaseFromBackup(): JQueryPromise<operationIdDto> {
        this.spinners.create(true);

        const restoreInfo = this.databaseModel.toRestoreDatabaseDto();

        return new restoreDatabaseFromBackupCommand(restoreInfo)
            .execute()
            .done((operationIdDto: operationIdDto) => {
                const operationId = operationIdDto.OperationId;
                notificationCenter.instance.openDetailsForOperationById(null, operationId);
            })
            .always(() => {
                dialog.close(this);
                this.spinners.create(false);
            });
    }

    findRestoreSourceLabel(restoreSource: restoreSource) {
        return this.databaseModel.restoreSourceObject().backupStorageTypeText;
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

    redirectToCertificates(){
        dialog.close(this);
        router.navigate(appUrl.forCertificates());       
    }
}

export = createDatabase;
