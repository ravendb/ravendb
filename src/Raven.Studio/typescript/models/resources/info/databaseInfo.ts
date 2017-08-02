/// <reference path="../../../../typings/tsd.d.ts"/>

import database = require("models/resources/database");
import databasesManager = require("common/shell/databasesManager");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import generalUtils = require("common/generalUtils");
import databaseGroupNode = require("models/resources/info/databaseGroupNode");

class databaseInfo {

    name: string;

    uptime = ko.observable<string>();
    totalSize = ko.observable<string>();
    bundles = ko.observableArray<string>();
    backupStatus = ko.observable<string>();
    lastFullOrIncrementalBackup = ko.observable<string>();

    loadError = ko.observable<string>();

    isAdmin = ko.observable<boolean>();
    disabled = ko.observable<boolean>();
    backupEnabled = ko.observable<boolean>();

    licensed = ko.observable<boolean>(true); //TODO: bind this value  
    filteredOut = ko.observable<boolean>(false);
    isBeingDeleted = ko.observable<boolean>(false);

    indexingErrors = ko.observable<number>();
    alerts = ko.observable<number>();

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

    constructor(dto: Raven.Client.Server.Operations.DatabaseInfo) {
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

    private computeBackupStatus(dto: Raven.Client.Server.Operations.BackupInfo) {
        if (!dto.LastBackup) {
            return "text-danger";
        }

        return dto.IntervalUntilNextBackupInSec === 0 ? "text-warning" : "text-success";
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

            if (!this.licensed()) {
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

            if (!this.licensed()) {
                return "Unlicensed";
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
            const hasLicense = this.licensed();
            const enabled = !this.disabled();
            const hasLoadError = this.hasLoadError();
            return hasLicense && enabled && !hasLoadError;
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

    update(dto: Raven.Client.Server.Operations.DatabaseInfo): void {
        this.name = dto.Name;
        this.disabled(dto.Disabled);
        this.isAdmin(dto.IsAdmin);
        this.totalSize(dto.TotalSize ? dto.TotalSize.HumaneSize : null);
        this.indexingErrors(dto.IndexingErrors);
        this.alerts(dto.Alerts);
        this.loadError(dto.LoadError);
        this.uptime(generalUtils.timeSpanAsAgo(dto.UpTime, false));
        this.backupEnabled(!!dto.BackupInfo);
        if (this.backupEnabled()) {
            this.lastFullOrIncrementalBackup(moment.utc(dto.BackupInfo.LastBackup).local().fromNow());
            this.backupStatus(this.computeBackupStatus(dto.BackupInfo));
        }

        this.rejectClients(dto.RejectClients);
        this.indexingStatus(dto.IndexingStatus);
        this.indexingDisabled(dto.IndexingStatus === "Disabled");
        this.indexingPaused(dto.IndexingStatus === "Paused");
        this.documentsCount(dto.DocumentsCount);
        this.indexesCount(dto.IndexesCount);

        const topologyDto = dto.NodesTopology;
        const members = this.mapNodes("Member", topologyDto.Members);
        const promotables = this.mapNodes("Promotable", topologyDto.Promotables);
        const rehabs = this.mapNodes("Rehab", topologyDto.Rehabs);

        const joinedNodes = _.concat<databaseGroupNode>(members, promotables, rehabs);
        this.applyNodesStatuses(joinedNodes, topologyDto.Status);

        this.nodes(joinedNodes);
        //TODO: consider in place update? of nodes?
    }

    private applyNodesStatuses(nodes: databaseGroupNode[], statuses: { [key: string]: Raven.Client.Server.Operations.DbGroupNodeStatus;}) {
        nodes.forEach(node => {
            if (node.tag() in statuses) {
                const nodeStatus = statuses[node.tag()];
                node.lastStatus(nodeStatus.LastStatus);
                node.lastError(nodeStatus.LastError);
            }
        });
    }

    private mapNodes(type: databaseGroupNodeType, nodes: Array<Raven.Client.Server.Operations.NodeId>): Array<databaseGroupNode> {
        return _.map(nodes, v => databaseGroupNode.for(v.NodeTag, v.NodeUrl, v.ResponsibleNode, type));
    }
}

export = databaseInfo;
