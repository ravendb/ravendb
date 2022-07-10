/*


class ongoingTaskBackupListModel extends ongoingTaskListModel {
    private static neverBackedUpText = "Never backed up";
    
    static serverWideNamePrefixFromServer = "Server Wide Backup";
    
    activeDatabase = activeDatabaseTracker.default.database;
    
    private watchProvider: (task: ongoingTaskBackupListModel) => void;

    backupType = ko.observable<Raven.Client.Documents.Operations.Backups.BackupType>();
    nextBackup = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.NextBackup>();
    lastFullBackup = ko.observable<string>();
    lastIncrementalBackup = ko.observable<string>();
    onGoingBackup = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.RunningBackup>();
    retentionPolicyPeriod = ko.observable<string>();
    retentionPolicyDisabled = ko.observable<boolean>();

    textClass = ko.observable<string>();

    backupDestinations = ko.observableArray<string>([]);
    backupNowInProgress = ko.observable<boolean>(false);
    isRunningOnAnotherNode: KnockoutComputed<boolean>;
    disabledBackupNowReason = ko.observable<string>();
    isBackupNowEnabled: KnockoutComputed<boolean>;
    isBackupNowVisible: KnockoutComputed<boolean>;
    neverBackedUp = ko.observable<boolean>(false);
    fullBackupTypeName: KnockoutComputed<string>;
    isBackupEncrypted = ko.observable<boolean>();
    lastExecutingNode = ko.observable<string>();

    backupDestinationsHumanized: KnockoutComputed<string>;
    lastFullBackupHumanized: KnockoutComputed<string>;
    lastIncrementalBackupHumanized: KnockoutComputed<string>;
    nextBackupHumanized: KnockoutComputed<string>;
    onGoingBackupHumanized: KnockoutComputed<string>;
    retentionPolicyHumanized: KnockoutComputed<string>;
    throttledRefreshBackupInfo: () => void;

    get studioTaskType(): StudioTaskType {
        return "Backup";
    }
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup, watchProvider: (task: ongoingTaskBackupListModel) => void) {
        super();
        
        this.throttledRefreshBackupInfo = _.throttle(() => this.refreshBackupInfo(false), 60 * 1000);
        
        this.watchProvider = watchProvider;
        this.update(dto);
        
        this.initializeObservables();
    }

    initializeObservables() {
        super.initializeObservables();
        
        this.editUrl = ko.pureComputed(()=> {
             const urls = appUrl.forCurrentDatabase();

             return urls.editPeriodicBackupTask(this.taskId)();
        });

        this.isBackupNowEnabled = ko.pureComputed(() => {
            if (this.nextBackupHumanized() === "N/A") {
                this.disabledBackupNowReason("No backup destinations");
                return false;
            }

            if (this.isRunningOnAnotherNode()) {
                // the backup is running on another node
                this.disabledBackupNowReason(`Backup in progress on node ${this.responsibleNode().NodeTag}`);
                return false;
            }

            this.disabledBackupNowReason(null);
            return true;
        });

        this.isBackupNowVisible = ko.pureComputed(() => {
            return  !this.isServerWide() || accessManager.default.isClusterAdminOrClusterNode();
        });
        
        this.lastFullBackupHumanized = ko.pureComputed(() => {
            const lastFullBackup = this.lastFullBackup();
            if (!lastFullBackup) {
                return ongoingTaskBackupListModel.neverBackedUpText;
            }

            return generalUtils.formatDurationByDate(moment.utc(lastFullBackup), true);
        });

        this.lastIncrementalBackupHumanized = ko.pureComputed(() => {
            const lastIncrementalBackup = this.lastIncrementalBackup();
            if (!lastIncrementalBackup) {
                return ongoingTaskBackupListModel.neverBackedUpText;
            }

            return generalUtils.formatDurationByDate(moment.utc(lastIncrementalBackup), true);
        });

        this.nextBackupHumanized = ko.pureComputed(() => {
            const nextBackup = this.nextBackup();
            if (!nextBackup) {
                this.textClass("text-warning");
                return "N/A";
            }

            if (this.isRunningOnAnotherNode()) {
                this.textClass("text-info");
                // the backup is running on another node
                return `Backup is already running or should start shortly on node ${this.responsibleNode().NodeTag}`;
            }

            this.textClass("text-details");
            const now = timeHelpers.utcNowWithSecondPrecision();
            const diff = moment.utc(nextBackup.DateTime).diff(now);
            
            if (diff <= 0 && this.showDetails()) {
                this.throttledRefreshBackupInfo();
            }

            const backupType = this.getBackupType(this.backupType(), nextBackup.IsFull);
            const backupTypeText = backupType !== "Snapshot" ? `${backupType} Backup` : backupType;
            const formatDuration = generalUtils.formatDuration(moment.duration(diff), true, 2, true);
            return `in ${formatDuration} (${backupTypeText})`;
        });

        this.onGoingBackupHumanized = ko.pureComputed(() => {
            const onGoingBackup = this.onGoingBackup();
            if (!onGoingBackup) {
                return null;
            }

            const fromDuration = generalUtils.formatDurationByDate(moment.utc(onGoingBackup.StartTime), true);
            return `${fromDuration} (${this.getBackupType(this.backupType(), onGoingBackup.IsFull)})`;
        });

        this.retentionPolicyHumanized = ko.pureComputed(() => {
            return this.retentionPolicyDisabled() ? "No backups will be removed" : generalUtils.formatTimeSpan(this.retentionPolicyPeriod(), true);
        });
        
        this.backupDestinationsHumanized =  ko.pureComputed(() => {
            return this.backupDestinations().length ? this.backupDestinations().join(', ') : "No destinations defined";
        });

        this.isRunningOnAnotherNode = ko.pureComputed(() => {
            const responsibleNode = this.responsibleNode();
            if (!responsibleNode || !responsibleNode.NodeTag) {
                return false;
            }

            if (responsibleNode.NodeTag === clusterTopologyManager.default.localNodeTag()) {
                return false;
            }

            const nextBackup = this.nextBackup();
            if (!nextBackup) {
                return false;
            }

            const now = timeHelpers.utcNowWithSecondPrecision();
            const diff = moment.utc(nextBackup.DateTime).diff(now);
            return diff <= 0;
        });

        this.fullBackupTypeName = ko.pureComputed(() => this.getBackupType(this.backupType(), true));
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup) {
        super.update(dto);

        this.backupType(dto.BackupType);
        this.backupDestinations(dto.BackupDestinations);
        
        this.neverBackedUp(!dto.LastFullBackup);
        this.lastFullBackup(dto.LastFullBackup);
        this.lastIncrementalBackup(dto.LastIncrementalBackup);
        this.nextBackup(dto.NextBackup);
        this.onGoingBackup(dto.OnGoingBackup);
        this.isBackupEncrypted(dto.IsEncrypted);
        this.lastExecutingNode(dto.LastExecutingNodeTag || "N/A");

        // Check backward compatibility
        this.retentionPolicyDisabled(dto.RetentionPolicy ? dto.RetentionPolicy.Disabled : true);
        this.retentionPolicyPeriod(dto.RetentionPolicy ? dto.RetentionPolicy.MinimumBackupAgeToKeep : "0.0:00:00");
        
        if (this.onGoingBackup()) {
            this.watchProvider(this);
        }

        this.backupNowInProgress(!!this.onGoingBackup());
        
        this.isServerWide(this.taskName().startsWith(ongoingTaskBackupListModel.serverWideNamePrefixFromServer));
    }

    private getBackupType(backupType: Raven.Client.Documents.Operations.Backups.BackupType, isFull: boolean): string {
        if (!isFull) {
            return "Incremental";
        }

        if (backupType === "Snapshot") {
            return "Snapshot";
        }

        return "Full";
    }

    toggleDetails() {
        this.showDetails.toggle();

        if (this.showDetails()) {
            this.refreshBackupInfo(true);
        } 
    }

    refreshBackupInfo(reportFailure: boolean) {
        if (connectionStatus.showConnectionLost()) {
            // looks like we don't have connection to server, skip index progress update 
            return $.Deferred<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup>().fail();
        }

        return ongoingTaskInfoCommand.forBackup(this.activeDatabase(), this.taskId, reportFailure)
            .execute()
            .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup) => this.update(result));
    }

    backupNow() {
        const db = this.activeDatabase();
        const onGoingBackup = this.onGoingBackup();
        if (onGoingBackup) {
            const runningOperationId = onGoingBackup.RunningBackupTaskId;
            if (runningOperationId) {
                notificationCenter.instance.openDetailsForOperationById(db, runningOperationId);
                return;
            }
        }

        const backupNowViewModel = new backupNow(this.getBackupType(this.backupType(), true));
        backupNowViewModel
            .result
            .done((confirmResult: backupNowConfirmResult) => {
                if (confirmResult.can) {
                    this.backupNowInProgress(true);

                    const task = new backupNowPeriodicCommand(this.activeDatabase(), this.taskId, confirmResult.isFullBackup, this.taskName());
                    task.execute()
                        .done((backupNowResult: Raven.Client.Documents.Operations.Backups.StartBackupOperationResult) => {
                            this.refreshBackupInfo(true);
                            this.watchProvider(this);

                            if (backupNowResult && clusterTopologyManager.default.localNodeTag() === backupNowResult.ResponsibleNode) {
                                // running on this node
                                const operationId = backupNowResult.OperationId;
                                if (!this.onGoingBackup()) {
                                    this.onGoingBackup({
                                        IsFull: confirmResult.isFullBackup,
                                        RunningBackupTaskId: operationId
                                    });
                                }
                                notificationCenter.instance.openDetailsForOperationById(db, operationId);
                            }
                        })
                        .fail(() => {
                            // we failed to start the backup task
                            this.backupNowInProgress(false);
                        });
                        // backupNowInProgress is set to false after operation is finished
                }
            });

        app.showBootstrapDialog(backupNowViewModel);
    }
}


class ongoingTasks extends shardViewModelBase {
    private watchedBackups = new Map<number, number>();

 this.addNotification(this.changesContext.serverNotifications()
            .watchClusterTopologyChanges(() => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications()
            .watchDatabaseChange(this.db?.name, () => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications().watchReconnect(() => this.refresh()));

    createResponsibleNodeUrl(task: ongoingTaskListModel) {
        return ko.pureComputed(() => {
            const node = task.responsibleNode();
            const db = this.db;

            if (node && db) {
                return node.NodeUrl + appUrl.forOngoingTasks(db);
            }

            return "#";
        });
    }

    private refresh() {
        if (!this.db) {
            return;
        }
        return $.when<any>(this.fetchDatabaseInfo(), this.fetchOngoingTasks());
    }

    private watchBackupCompletion(task: ongoingTaskBackupListModel) {
        if (!this.watchedBackups.has(task.taskId)) {
            let intervalId = setInterval(() => {
                task.refreshBackupInfo(false)
                    .done(result => {
                        if (!result.OnGoingBackup) {
                            clearInterval(intervalId);
                            intervalId = null;
                            this.watchedBackups.delete(task.taskId);
                        }
                    })
            }, 3000);
            this.watchedBackups.set(task.taskId, intervalId as unknown as number);

            this.registerDisposable({
                dispose: () => {
                    if (intervalId) {
                        clearInterval(intervalId);
                        intervalId = null;
                        this.watchedBackups.delete(task.taskId);
                    }
                }
            });
        }
    }

    private processTasksResult(result: Raven.Server.Web.System.OngoingTasksResult) {
        const groupedTasks = _.groupBy(result.OngoingTasksList, x => x.TaskType);

        // Sort external replication tasks
        const groupedReplicationTasks = _.groupBy(this.replicationTasks(), x => x.isServerWide());
        const serverWideReplicationTasks = groupedReplicationTasks.true;
        const ongoingReplicationTasks = groupedReplicationTasks.false;

        if (ongoingReplicationTasks) {
            this.replicationTasks(serverWideReplicationTasks ? ongoingReplicationTasks.concat(serverWideReplicationTasks) : ongoingReplicationTasks);
        } else if (serverWideReplicationTasks) {
            this.replicationTasks(serverWideReplicationTasks);
        }

        this.mergeTasks(this.backupTasks,
            groupedTasks["Backup" as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType],
            toDeleteIds,
            (dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup) => new ongoingTaskBackupListModel(dto, task => this.watchBackupCompletion(task)));

        // Sort backup tasks 
        const groupedBackupTasks = _.groupBy(this.backupTasks(), x => x.isServerWide());
        const serverWideBackupTasks = groupedBackupTasks.true;
        const ongoingBackupTasks = groupedBackupTasks.false;

        if (ongoingBackupTasks) {
            this.backupTasks(serverWideBackupTasks ? ongoingBackupTasks.concat(serverWideBackupTasks) : ongoingBackupTasks);
        } else if (serverWideBackupTasks) {
            this.backupTasks(serverWideBackupTasks);
        }

        const hubOngoingTasks = groupedTasks["PullReplicationAsHub" as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType] as unknown as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub[];
        this.mergeReplicationHubs(result.PullReplications, hubOngoingTasks || [], toDeleteIds);

        const taskTypes = Object.keys(groupedTasks);

        if ((hubOngoingTasks || []).length === 0 && result.PullReplications.length) {
            // we have any pull replication definitions but no incoming connections, so append PullReplicationAsHub task type
            taskTypes.push("PullReplicationAsHub" as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType);
        }

    
    }

    private mergeReplicationHubs(incomingDefinitions: Array<Raven.Client.Documents.Operations.Replication.PullReplicationDefinition>,
                                 incomingData: Array<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub>,
                                 toDelete: Array<number>) {

        const container = this.replicationHubTasks;

        // remove old hub tasks
        container()
            .filter(x => _.includes(toDelete, x.taskId))
            .forEach(task => container.remove(task));

        (incomingDefinitions || []).forEach(item => {
            const existingItem = container().find(x => x.taskId === item.TaskId);
            if (existingItem) {
                existingItem.update(item);
                existingItem.updateChildren(incomingData.filter(x => x.TaskId === item.TaskId));
            } else {
                const newItem = new ongoingTaskReplicationHubDefinitionListModel(item);
                const insertIdx = _.sortedIndexBy(container(), newItem, x => x.taskName().toLocaleLowerCase());
                container.splice(insertIdx, 0, newItem);
                newItem.updateChildren(incomingData.filter(x => x.TaskId === item.TaskId));
            }
        });
    }

    manageDatabaseGroupUrl(dbInfo: databaseInfo): string {
        return appUrl.forManageDatabaseGroup(dbInfo);
    }

}

TODO: this become node spefic
<script type="text/html" id="responsible-node-template">
    <div data-bind="with: responsibleNode().NodeTag, visible: !usingNotPreferredNode(),
                    attr: { title: taskType() === 'PullReplicationAsHub' ? 'Hub node that is serving this Sink task' : 'Cluster node that is responsible for this task' }">
        <i class="icon-cluster-node"></i>
        <span data-bind="text: $data"></span>
    </div>
    <div data-bind="with: responsibleNode().NodeTag, visible: usingNotPreferredNode()">
        <i class="icon-cluster-node"></i>
        <span class="text-danger pulse" data-bind="text: $parent.mentorNode" title="User preferred node for this task"></span>
        <i class="icon-arrow-right pulse text-danger"></i>
        <span class="text-success" data-bind="text: $data" title="Cluster node that is temporary responsible for this task"></span>
    </div>
    <div data-bind="if: !responsibleNode().NodeTag" title="No node is currently handling this task">
        <i class="icon-cluster-node"></i> N/A
    </div>
</script>

type PerConnectionStats = {
    clientUri: string;
    workerId: string;
    strategy?: Raven.Client.Documents.Subscriptions.SubscriptionOpeningStrategy;
}


class ongoingTaskReplicationHubDefinitionListModel {
    
    taskId: number;
    taskName = ko.observable<string>();
    taskState = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState>();

    showDelayReplication = ko.observable<boolean>(false);
    delayReplicationTime = ko.observable<number>();
    delayHumane: KnockoutComputed<string>;
    
    ongoingHubs = ko.observableArray<ongoingTaskReplicationHubListModel>([]);

    editUrl: KnockoutComputed<string>;
    stateText: KnockoutComputed<string>;

    showDetails = ko.observable(false);
    isServerWide = ko.observable<boolean>(false);
  
    constructor(dto: Raven.Client.Documents.Operations.Replication.PullReplicationDefinition) {
        this.update(dto);

        this.initObservables();
    }

    taskType(): Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType {
        return "PullReplicationAsHub";
    }

    editTask() {
        router.navigate(this.editUrl());
    }
    
    private initObservables() {
        this.stateText = ko.pureComputed(() => this.taskState());

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editReplicationHub(this.taskId);

        this.delayHumane = ko.pureComputed(() => generalUtils.formatTimeSpan(this.delayReplicationTime() * 1000, true));
    }
    
    update(dto: Raven.Client.Documents.Operations.Replication.PullReplicationDefinition) {
        const delayTime = generalUtils.timeSpanToSeconds(dto.DelayReplicationFor);
        
        this.taskName(dto.Name);
        this.taskState(dto.Disabled ? "Disabled" : "Enabled");
        this.taskId = dto.TaskId;

        this.showDelayReplication(dto.DelayReplicationFor != null && delayTime !== 0);
        this.delayReplicationTime(dto.DelayReplicationFor ? delayTime : null);
    }

    updateChildren(ongoingTasks: Array<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub>) {
        const existingNames = this.ongoingHubs().map(x => x.uniqueName);
        
        ongoingTasks.forEach(incomingTask => {
           const uniqueName = ongoingTaskReplicationHubListModel.generateUniqueName(incomingTask); 
           const existingItem = this.ongoingHubs().find(x => x.uniqueName === uniqueName);
           if (existingItem) {
               existingItem.update(incomingTask);
               _.pull(existingNames, uniqueName);
           } else {
               this.ongoingHubs.push(new ongoingTaskReplicationHubListModel(incomingTask));
           }
        });
        
        existingNames.forEach(toDelete => {
            const item = this.ongoingHubs().find(x => x.uniqueName === toDelete);
            if (item) {
                this.ongoingHubs.remove(item);
            }
        });
    }
export = ongoingTaskReplicationHubDefinitionListModel;

class ongoingTaskRabbitMqEtlListModel extends abstractOngoingTaskEtlListModel {
    connectionStringDefined = ko.observable<boolean>(true); // needed for template in the ongoing tasks list view
    
    get studioTaskType(): StudioTaskType {
        return "RabbitQueueEtl";
    }
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlListView) {
        super();

        this.update(dto);
        this.initializeObservables();

        this.connectionStringsUrl = appUrl.forConnectionStrings(activeDatabaseTracker.default.database(), "RabbitMQ", this.connectionStringName());
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editRabbitMqEtl(this.taskId);
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlListView) {
        super.update(dto);

        this.connectionStringName(dto.ConnectionStringName);
    }
}

class ongoingTaskKafkaEtlListModel extends abstractOngoingTaskEtlListModel {
    bootstrapServers = ko.observable<string>();
    connectionStringDefined = ko.observable<boolean>(true); // needed for template in the ongoing tasks list view
    
    get studioTaskType(): StudioTaskType {
        return "KafkaQueueEtl";
    }
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlListView) {
        super();

        this.update(dto);
        this.initializeObservables();

        this.connectionStringsUrl = appUrl.forConnectionStrings(activeDatabaseTracker.default.database(), "Kafka", this.connectionStringName());
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editKafkaEtl(this.taskId);
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlListView) {
        super.update(dto);

        this.connectionStringName(dto.ConnectionStringName);
        this.bootstrapServers(dto.Url);
    }
}


*/
