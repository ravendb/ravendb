/// <reference path="../../../../typings/tsd.d.ts"/>

import database = require("models/resources/database");
import databasesManager = require("common/shell/databasesManager");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import generalUtils = require("common/generalUtils");
import databaseGroupNode = require("models/resources/info/databaseGroupNode");

class databaseInfo {

    private static dayAsSeconds = 60 * 60 * 24;

    name: string;

    uptime = ko.observable<string>();
    totalSize = ko.observable<number>();
    totalTempBuffersSize = ko.observable<number>();
    bundles = ko.observableArray<string>();
    backupStatus = ko.observable<string>();
    lastBackupText = ko.observable<string>();
    lastFullOrIncrementalBackup = ko.observable<string>();
    dynamicDatabaseDistribution = ko.observable<boolean>();
    priorityOrder = ko.observableArray<string>();

    loadError = ko.observable<string>();

    isEncrypted = ko.observable<boolean>();
    isAdmin = ko.observable<boolean>();
    disabled = ko.observable<boolean>();
    lockMode = ko.observable<Raven.Client.ServerWide.DatabaseLockMode>();

    filteredOut = ko.observable<boolean>(false);
    isBeingDeleted = ko.observable<boolean>(false);

    indexingErrors = ko.observable<number>();
    alerts = ko.observable<number>();
    performanceHints = ko.observable<number>();

    environment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>();
    
    badgeText: KnockoutComputed<string>;
    badgeClass: KnockoutComputed<string>;

    online: KnockoutComputed<boolean>;
    isLoading: KnockoutComputed<boolean>;
    hasLoadError: KnockoutComputed<boolean>;
    canNavigateToDatabase: KnockoutComputed<boolean>;
    isCurrentlyActiveDatabase: KnockoutComputed<boolean>;

    inProgressAction = ko.observable<string>();

    rejectClients = ko.observable<boolean>();
    indexingStatus = ko.observable<Raven.Client.Documents.Indexes.IndexRunningStatus>();
    indexingDisabled = ko.observable<boolean>();
    indexingPaused = ko.observable<boolean>();
    documentsCount = ko.observable<number>();
    indexesCount = ko.observable<number>();

    nodes = ko.observableArray<databaseGroupNode>([]);
    deletionInProgress = ko.observableArray<string>([]);

    constructor(dto: Raven.Client.ServerWide.Operations.DatabaseInfo) {
        this.initializeObservables();

        this.update(dto);
    }

    get qualifier() {
        return "db";
    }

    get fullTypeName() {
        return "database";
    }

    get urlPrefix() {
        return "databases";
    }

    asDatabase(): database {
        const casted = databasesManager.default.getDatabaseByName(this.name);
        if (!casted) {
            throw new Error("Unable to find database: " + this.name + " in database manager");
        }
        return casted;
    }

    static extractQualifierAndNameFromNotification(input: string): { qualifier: string, name: string } {
        return { qualifier: input.substr(0, 2), name: input.substr(3) };
    }

    private computeBackupStatus(backupInfo: Raven.Client.ServerWide.Operations.BackupInfo) {
        if (!backupInfo || !backupInfo.LastBackup) {
            this.lastBackupText("Never backed up");
            return "text-danger";
        }

        const dateInUtc = moment.utc(backupInfo.LastBackup);
        const diff = moment().utc().diff(dateInUtc);
        const durationInSeconds = moment.duration(diff).asSeconds();

        this.lastBackupText(`Backed up ${this.lastFullOrIncrementalBackup()}`);
        return durationInSeconds > databaseInfo.dayAsSeconds ? "text-warning" : "text-success";
    }
    
    isLocal(currentNodeTag: string) {
        return _.includes(this.nodes().map(x => x.tag()), currentNodeTag);
    }

    private initializeObservables() {
        this.hasLoadError = ko.pureComputed(() => !!this.loadError());

        this.online = ko.pureComputed(() => {
            return !!this.uptime();
        });

        this.badgeClass = ko.pureComputed(() => {
            if (this.hasLoadError()) {
                return "state-danger";
            }

            if (this.disabled()) {
                return "state-warning";
            }

            if (this.online()) {
                return "state-success";
            }

            return "state-offline"; // offline
        });

        this.badgeText = ko.pureComputed(() => {
            if (this.hasLoadError()) {
                return "Error";
            }

            if (this.disabled()) {
                return "Disabled";
            }

            if (this.uptime()) {
                return "Online";
            }
            return "Offline";
        });

        this.canNavigateToDatabase = ko.pureComputed(() => {
            const enabled = !this.disabled();
            const hasLoadError = this.hasLoadError();
            return enabled && !hasLoadError;
        });

        this.isCurrentlyActiveDatabase = ko.pureComputed(() => {
            const currentDatabase = activeDatabaseTracker.default.database();

            if (!currentDatabase) {
                return false;
            }

            return currentDatabase.name === this.name;
        });

        this.isLoading = ko.pureComputed(() => {
            return this.isCurrentlyActiveDatabase() &&
                !this.online() &&
                !this.disabled();
        });
    }

    update(dto: Raven.Client.ServerWide.Operations.DatabaseInfo): void {
        this.name = dto.Name;
        this.lockMode(dto.LockMode);
        this.disabled(dto.Disabled);
        this.isAdmin(dto.IsAdmin);
        this.isEncrypted(dto.IsEncrypted);
        this.totalSize(dto.TotalSize ? dto.TotalSize.SizeInBytes : 0);
        this.totalTempBuffersSize(dto.TempBuffersSize ? dto.TempBuffersSize.SizeInBytes : 0);
        this.indexingErrors(dto.IndexingErrors);
        this.alerts(dto.Alerts);
        this.performanceHints(dto.PerformanceHints);
        this.loadError(dto.LoadError);
        this.uptime(generalUtils.timeSpanAsAgo(dto.UpTime, false));
        this.dynamicDatabaseDistribution(dto.DynamicNodesDistribution);
        
        this.environment(dto.Environment);

        if (dto.BackupInfo && dto.BackupInfo.LastBackup) {
            this.lastFullOrIncrementalBackup(moment.utc(dto.BackupInfo.LastBackup).local().fromNow());
        }
            
        this.backupStatus(this.computeBackupStatus(dto.BackupInfo));

        this.rejectClients(dto.RejectClients);
        this.indexingStatus(dto.IndexingStatus);
        this.indexingDisabled(dto.IndexingStatus === "Disabled");
        this.indexingPaused(dto.IndexingStatus === "Paused");
        this.documentsCount(dto.DocumentsCount);
        this.indexesCount(dto.IndexesCount);
        this.deletionInProgress(dto.DeletionInProgress ? Object.keys(dto.DeletionInProgress) : []);

        const topologyDto = dto.NodesTopology;
        if (topologyDto) {
            const members = this.mapNodes("Member", topologyDto.Members);
            const promotables = this.mapNodes("Promotable", topologyDto.Promotables);
            const rehabs = this.mapNodes("Rehab", topologyDto.Rehabs);
            const joinedNodes = _.concat<databaseGroupNode>(members, promotables, rehabs);
            this.applyNodesStatuses(joinedNodes, topologyDto.Status);

            this.priorityOrder(topologyDto.PriorityOrder);
            
            this.nodes(joinedNodes);
        }
    }

    private applyNodesStatuses(nodes: databaseGroupNode[], statuses: { [key: string]: Raven.Client.ServerWide.Operations.DatabaseGroupNodeStatus;}) {
        nodes.forEach(node => {
            if (node.tag() in statuses) {
                const nodeStatus = statuses[node.tag()];
                node.lastStatus(nodeStatus.LastStatus);
                node.lastError(nodeStatus.LastError);
            }
        });
    }

    private mapNodes(type: databaseGroupNodeType, nodes: Array<Raven.Client.ServerWide.Operations.NodeId>): Array<databaseGroupNode> {
        return _.map(nodes, v => databaseGroupNode.for(v.NodeTag, v.NodeUrl, v.ResponsibleNode, type));
    }
}

export = databaseInfo;
