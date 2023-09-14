import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import databasesManager = require("common/shell/databasesManager");
import createDatabaseCommand = require("commands/resources/createDatabaseCommand");
import restoreDatabaseFromBackupCommand = require("commands/resources/restoreDatabaseFromBackupCommand");
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
import lastUsedAutocomplete = require("common/storage/lastUsedAutocomplete");
import viewModelBase = require("viewmodels/viewModelBase");
import studioSettings = require("common/settings/studioSettings");
import licenseModel = require("models/auth/licenseModel");
import generalUtils = require("common/generalUtils");
import accessManager = require("common/shell/accessManager");

class createDatabase extends dialogViewModelBase {
    
    view = require("views/resources/createDatabase.html");
    
    static readonly legacyKeySizes = [128, 192, 256];

    static readonly defaultSection: availableConfigurationSectionId = "replicationAndSharding";
    
    clientVersion = viewModelBase.clientVersion;

    spinners = {
        create: ko.observable<boolean>(false),
        databaseLocationInfoLoading: ko.observable<boolean>(false)
    };

    databaseModel: databaseCreationModel;
    clusterNodes: clusterNode[] = [];
    
    encryptionSection: setupEncryptionKey;
    isSecureServer = accessManager.default.secureServer();
    
    protected currentAdvancedSection = ko.observable<availableConfigurationSectionId>();

    showDynamicDatabaseDistributionWarning: KnockoutComputed<boolean>;
    showReplicationFactorWarning: KnockoutComputed<boolean>;
    showNumberOfShardsWarning: KnockoutComputed<boolean>;
    enforceManualNodeSelection: KnockoutComputed<boolean>;
    disableReplicationFactorInput: KnockoutComputed<boolean>;
    selectionState: KnockoutComputed<checkbox>;
    orchestratorSelectionState: KnockoutComputed<checkbox>;
    canUseDynamicOption: KnockoutComputed<boolean>; 
    defaultReplicationFactor = ko.observable<number>();

    licenseType = licenseModel.licenseType();
    isProfessionalOrAbove = licenseModel.isProfessionalOrAbove();
    isEnterpriseOrDeveloper = licenseModel.isEnterpriseOrDeveloper();

    databaseLocationInfo = ko.observableArray<Raven.Server.Web.Studio.SingleNodeDataDirectoryResult>([]);
    databaseLocationInfoToDisplay: KnockoutComputed<Raven.Server.Web.Studio.SingleNodeDataDirectoryResult[]>;
    databaseLocationInfoMessage: KnockoutComputed<string>;

    recentPathsAutocomplete: lastUsedAutocomplete;
    sourceJournalsPathOptions = ko.observableArray<string>([]);
    
    pathOptions = ko.observableArray<{ path: string, isRecent: boolean }>([]);

    getDatabaseByName(name: string): database {
        return databasesManager.default.getDatabaseByName(name);
    }

    advancedVisibility = {
        restore: ko.pureComputed(() => this.currentAdvancedSection() === "restore"),
        encryption: ko.pureComputed(() => this.currentAdvancedSection() === "encryption"),
        replicationAndSharding: ko.pureComputed(() => this.currentAdvancedSection() === "replicationAndSharding"),
        path: ko.pureComputed(() => this.currentAdvancedSection() === "path")
    };

    constructor(mode: dbCreationMode) {
        super();

        const canCreateEncryptedDatabases = ko.pureComputed(() => this.isSecureServer && licenseModel.licenseStatus() && licenseModel.licenseStatus().HasEncryption);
        this.databaseModel = new databaseCreationModel(mode, canCreateEncryptedDatabases);
        this.recentPathsAutocomplete = new lastUsedAutocomplete("createDatabasePath", this.databaseModel.path.dataPath);
        
        switch (mode) {
            case "newDatabase": 
                this.currentAdvancedSection(createDatabase.defaultSection);
                break;
            case "restore":
                this.currentAdvancedSection("restore");
                break;
        }
        
        this.encryptionSection = setupEncryptionKey.forDatabase(this.databaseModel.encryption.key, this.databaseModel.encryption.confirmation, this.databaseModel.name);

        this.bindToCurrentInstance("showAdvancedConfigurationFor", "toggleSelectAll", "orchestratorToggleSelectAll", "setShardTopologyNode", "getShardTopologyNode");
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
        
        this.updatePathOptions(this.databaseModel.path.dataPath());

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
        
        $('.restore [data-toggle="tooltip"]').tooltip();
        
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

        if (this.databaseModel.isFromBackup) {
            this.databaseModel.replicationAndSharding.replicationFactor(1);
        }
        
        // if we have single node - select it to make things easier
        if (this.clusterNodes.length === 1) {
            this.databaseModel.replicationAndSharding.nodes([this.clusterNodes[0]]);
        }
        
        this.databaseModel.addOnRestorePointChanged(restorePoint => {
            this.encryptionSection.canProvideOwnKey(!restorePoint || !restorePoint.isEncrypted || !restorePoint.isSnapshotRestore);
        })
        
        this.databaseModel.restore.backupEncryptionKey.subscribe(key => {
            const restorePoint = this.databaseModel.restore.restoreSourceObject()?.items()[0]?.selectedRestorePoint();
            if (restorePoint && restorePoint.isEncrypted && restorePoint.isSnapshotRestore) {
                // update database encryption key to match backup encryption key
                this.databaseModel.encryption.key(key);
            }
        });
    }

    private setDefaultReplicationFactor(topology: clusterTopology) {
        this.clusterNodes = topology.nodes();
        
        const defaultReplicationFactor = this.defaultReplicationFactor() || this.clusterNodes.length;
        this.databaseModel.replicationAndSharding.replicationFactor(defaultReplicationFactor);
        
        // also preset orchestrators in case of sharding
        this.databaseModel.replicationAndSharding.orchestrators(this.clusterNodes.slice());
    }

    protected initObservables() {
        this.canUseDynamicOption = ko.pureComputed(() => {
            const fromBackup = this.databaseModel.isFromBackup;
            const enforceManual = this.enforceManualNodeSelection();
            const replicationFactor = this.databaseModel.replicationAndSharding.replicationFactor();
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
            // eslint-disable-next-line no-prototype-builtins
            if (!section.hasOwnProperty('validationGroup')) {
                section.validationGroup = undefined;
            }
        });

        const encryption = this.databaseModel.configurationSections.find(x => x.id === "encryption");
        encryption.enabled.subscribe(encryptionEnabled => {
            const creationMode = this.databaseModel.creationMode;
            const canUseManualMode = creationMode === "newDatabase";
            if (encryptionEnabled && canUseManualMode) {
                this.databaseModel.replicationAndSharding.dynamicMode(false);
                this.databaseModel.replicationAndSharding.manualMode(true);
            }
        });

        this.showDynamicDatabaseDistributionWarning = ko.pureComputed(() => {
            const hasDynamicDatabaseDistribution = licenseModel.licenseStatus().HasDynamicNodesDistribution;
            if (!hasDynamicDatabaseDistribution) {
                this.databaseModel.replicationAndSharding.dynamicMode(false);
            }
            return !hasDynamicDatabaseDistribution;
        });

        this.showReplicationFactorWarning = ko.pureComputed(() => {
            const factor = this.databaseModel.replicationAndSharding.replicationFactor();
            return factor === 1;
        });

        this.showNumberOfShardsWarning = ko.pureComputed(() => {
            const shards = this.databaseModel.replicationAndSharding.numberOfShards();
            return shards === 0;
        });

        this.enforceManualNodeSelection = ko.pureComputed(() => {
            const fromBackup = this.databaseModel.creationMode !== "newDatabase";
            const encryptionEnabled = this.databaseModel.getEncryptionConfigSection().enabled();

            return encryptionEnabled || fromBackup;
        });

        this.disableReplicationFactorInput = ko.pureComputed(() => {
            const manual = this.databaseModel.replicationAndSharding.manualMode();
            const sharded = this.databaseModel.replicationAndSharding.enableSharding();
            return manual && !sharded;
        });

        this.selectionState = ko.pureComputed<checkbox>(() => {
            const clusterNodes = this.clusterNodes;
            const selectedCount = this.databaseModel.replicationAndSharding.nodes().length;

            if (clusterNodes.length && selectedCount === clusterNodes.length)
                return "checked";
            if (selectedCount > 0)
                return "some_checked";
            return "unchecked";
        });

        this.orchestratorSelectionState = ko.pureComputed<checkbox>(() => {
            const clusterNodes = this.clusterNodes;
            const selectedCount = this.databaseModel.replicationAndSharding.orchestrators().length;

            if (clusterNodes.length && selectedCount === clusterNodes.length)
                return "checked";
            if (selectedCount > 0)
                return "some_checked";
            return "unchecked";
        });

        this.databaseModel.path.dataPath.throttle(300).subscribe(newPath => {
            if (this.databaseModel.path.dataPath.isValid()) {
                this.updateDatabaseLocationInfo(this.databaseModel.name(), newPath);
                this.updatePathOptions(this.databaseModel.path.dataPath());
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
        
        
        this.databaseModel.restore.azureCredentials().registerWatchers();
        this.databaseModel.restore.amazonS3Credentials().registerWatchers();
        this.databaseModel.restore.googleCloudCredentials().registerWatchers();
        this.databaseModel.restore.localServerCredentials().registerWatchers();
        
        this.databaseModel.restore.restoreSourceObject.subscribe((obj) => {
            obj.updateBackupDirectoryPathOptions();
        });

        this.databaseLocationInfoToDisplay = ko.pureComputed(() => {
            const databaseLocationInfo = this.databaseLocationInfo();
            const selectedNodes = this.databaseModel.replicationAndSharding.nodes().map(x => x.tag());
            const replicationFactor = this.databaseModel.replicationAndSharding.replicationFactor();
            if (this.databaseModel.replicationAndSharding.manualMode()) {
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
            const selectedNodes = this.databaseModel.replicationAndSharding.nodes().map(x => x.tag());
            if (this.databaseModel.replicationAndSharding.manualMode()) {
                if (selectedNodes.length === 0) {
                    return null;
                }

                return `${message}:`;
            }

            const numberOfClusterNodes = this.clusterNodes.length;
            const replicationFactor = Math.min(this.databaseModel.replicationAndSharding.replicationFactor(), numberOfClusterNodes);
            if (replicationFactor === 0) {
                return null;
            }

            if (replicationFactor === 1 ||
                replicationFactor === numberOfClusterNodes) {
                return `${message}:`;
            }

            return `${message} ${replicationFactor} of the ${numberOfClusterNodes} nodes:`;
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
    
    updatePathOptions(dataPath: string) {
        const result: { path: string, isRecent: boolean }[] = [];
        
        this.getLocalFolderPaths(dataPath)
            .done((localFolderPaths: Raven.Server.Web.Studio.FolderPathOptions) => {

                const recentPaths = this.recentPathsAutocomplete.createCompleter();
                recentPaths().forEach(p => {
                    result.push({ path: p, isRecent: true });
                });

                localFolderPaths.List.forEach((p: string) => {
                    result.push({ path: p, isRecent: false });
                });
                
                this.pathOptions(result);
            });
    }

    private getLocalFolderPaths(path: string, backupFolder = false): JQueryPromise<Raven.Server.Web.Studio.FolderPathOptions> {
        return getFolderPathOptionsCommand.forServerLocal(path, backupFolder)
            .execute();
    }

    updateSourceJournalsPathOptions(path: string) {
        this.getLocalFolderPaths(path)
            .done(result => this.sourceJournalsPathOptions(result.List));
    }

    getAvailableSections() {
        const sections = this.databaseModel.configurationSections;

        const restoreSection = sections.find(x => x.id === "restore");
        
        switch (this.databaseModel.creationMode) {
            case "newDatabase":
                return _.without(sections, restoreSection);
            case "restore":
                sections.find(x => x.id === "replicationAndSharding").name = "Replication";
                return _.without(sections);
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

            let allValid = globalValid && _.every(sectionsValidityList, x => !!x);
            
            if (this.databaseModel.creationMode === "restore") {
                const source = this.databaseModel.restore.restoreSourceObject();
                if (!source.isValid()) {
                    allValid = false;
                }
                
                if (!source.isItemsValid()) {
                    allValid = false;
                }
            }

            if (allValid) {
                // disable validation for name as it might display error: database already exists
                // since we get async notifications during db creation
                this.databaseModel.name.extend({ validatable: false });
                
                this.recentPathsAutocomplete.recordUsage();

                switch (this.databaseModel.creationMode) {
                    case "restore":
                        this.createDatabaseFromBackup();
                        break;
                    case "newDatabase":
                        this.createDatabaseInternal();
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

    private createDatabaseInternal(shouldActive = true): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasePutResult> {
        this.spinners.create(true);

        const databaseDocument = this.databaseModel.toDto();
        const replicationFactor = this.databaseModel.replicationAndSharding.replicationFactor();

        if (shouldActive) {
            databasesManager.default.activateAfterCreation(databaseDocument.DatabaseName);
        }

        const encryptionTask = $.Deferred<void>();

        const encryptionSection = this.databaseModel.configurationSections.find(x => x.id === "encryption");
        if (encryptionSection.enabled()) {
            const nodeTags = this.databaseModel.replicationAndSharding.nodes().map(x => x.tag());
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

    findRestoreSourceLabel() {
        return this.databaseModel.restore.restoreSourceObject().backupStorageTypeText;
    }

    toggleSelectAll() {
        const replicationConfig = this.databaseModel.replicationAndSharding;
        const selectedCount = replicationConfig.nodes().length;

        if (selectedCount > 0) {
            replicationConfig.nodes([]);
        } else {
            replicationConfig.nodes(this.clusterNodes.slice());
        }
    }

    orchestratorToggleSelectAll() {
        const replicationConfig = this.databaseModel.replicationAndSharding;
        const selectedCount = replicationConfig.orchestrators().length;

        if (selectedCount > 0) {
            replicationConfig.orchestrators([]);
        } else {
            replicationConfig.orchestrators(this.clusterNodes.slice());
        }
    }

    redirectToCertificates(){
        dialog.close(this);
        router.navigate(appUrl.forCertificates());
    }
    
    setShardTopologyNode(shardNumber: number, replica: number, node: clusterNode): void {
        const topology = this.databaseModel.replicationAndSharding.shardTopology();
        const shardTopology = topology[shardNumber];
        if (!shardTopology) {
            return;
        }
        
        const replicasCopy = shardTopology.replicas().slice();
        replicasCopy[replica] = node;
        shardTopology.replicas(replicasCopy);
    }
    
    getShardTopologyNode(shardNumber: number, replica: number): string {
        const topology = this.databaseModel.replicationAndSharding.shardTopology();
        const shardTopology = topology[shardNumber];
        if (!shardTopology) {
            return null;
        }
        
        return shardTopology.replicas()[replica]?.tag() ?? null;
    }
}

export = createDatabase;
