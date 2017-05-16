/// <reference path="../../../../typings/tsd.d.ts"/>

import database = require("models/resources/database");
import databasesManager = require("common/shell/databasesManager");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import generalUtils = require("common/generalUtils");
import clusterNode = require("models/database/cluster/clusterNode");

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

    nodes = ko.observableArray<clusterNode>([]);

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
        return databasesManager.default.getDatabaseByName(this.name);
    }

    static extractQualifierAndNameFromNotification(input: string): { qualifier: string, name: string } {
        return { qualifier: input.substr(0, 2), name: input.substr(3) };
    }

    static findLastBackupDate(dto: Raven.Client.Server.Operations.BackupInfo) {
        const lastFull = dto.LastFullBackup;
        const lastIncrementalBackup = dto.LastIncrementalBackup;

        if (lastFull && lastIncrementalBackup) {
            return lastFull > lastIncrementalBackup ? lastFull : lastIncrementalBackup;
        } else if (lastFull) {
            return lastFull;
        }
        return lastIncrementalBackup;
    }

    private computeBackupStatus(dto: Raven.Client.Server.Operations.BackupInfo) {
        if (!dto.LastFullBackup && !dto.LastIncrementalBackup) {
            return "text-danger";
        }

        const fullBackupInterval = moment.duration(dto.FullBackupInterval).asSeconds();
        const incrementalBackupInterval = moment.duration(dto.IncrementalBackupInterval).asSeconds();

        const interval = (incrementalBackupInterval === 0) ? fullBackupInterval : Math.min(incrementalBackupInterval, fullBackupInterval);

        const lastBackup = new Date(databaseInfo.findLastBackupDate(dto));

        const secondsSinceLastBackup = moment.duration(moment().diff(moment(lastBackup))).asSeconds();

        return (interval * 1.2 < secondsSinceLastBackup) ? "text-warning" : "text-success";
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
        this.bundles(dto.Bundles);
        this.uptime(generalUtils.timeSpanAsAgo(dto.UpTime, false));
        this.backupEnabled(!!dto.BackupInfo);
        if (this.backupEnabled()) {
            const lastBackup = databaseInfo.findLastBackupDate(dto.BackupInfo);
            this.lastFullOrIncrementalBackup(moment(new Date(lastBackup)).fromNow());
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
        const watchers = this.mapNodes("Watcher", topologyDto.Watchers);

        this.nodes(_.concat<clusterNode>(members, promotables, watchers));
        //TODO: consider in place update? of nodes?
    }

    private mapNodes(type: clusterNodeType, nodes: Array<Raven.Client.Server.Operations.NodeId>): Array<clusterNode> {
        return _.map(nodes, v => clusterNode.for(v.NodeTag, v.NodeUrl, type));
    }
}

export = databaseInfo;
