/*
class ongoingTasks extends shardViewModelBase {
    private watchedBackups = new Map<number, number>();
    private etlProgressWatch: ReturnType<typeof setTimeout>;
    private definitionsCache: etlScriptDefinitionCache;

    activate(args: any): JQueryPromise<any> {
        super.activate(args);

        return $.when<any>(this.fetchDatabaseInfo(), this.fetchOngoingTasks());
    }

    attached() {
        super.attached();

        this.addNotification(this.changesContext.serverNotifications()
            .watchClusterTopologyChanges(() => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications()
            .watchDatabaseChange(this.db?.name, () => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications().watchReconnect(() => this.refresh()));
    }

    compositionComplete(): void {
        super.compositionComplete();

        this.definitionsCache = new etlScriptDefinitionCache(this.db);
    }

    private fetchEtlProcess() {
        return new etlProgressCommand(this.db)
            .execute()
            .done(results => {
                results.Results.forEach(taskProgress => {
                    switch (taskProgress.EtlType) {
                        case "Sql":
                            const matchingSqlTask = this.sqlEtlTasks().find(x => x.taskName() === taskProgress.TaskName);
                            if (matchingSqlTask) {
                                matchingSqlTask.updateProgress(taskProgress);
                            }
                            break;
                        case "Olap":
                            const matchingOlapTask = this.olapEtlTasks().find(x => x.taskName() === taskProgress.TaskName);
                            if (matchingOlapTask) {
                                matchingOlapTask.updateProgress(taskProgress);
                            }
                            break;
                        case "Raven":
                            const matchingRavenTask = this.ravenEtlTasks().find(x => x.taskName() === taskProgress.TaskName);
                            if (matchingRavenTask) {
                                matchingRavenTask.updateProgress(taskProgress);
                            }
                            break;
                        case "ElasticSearch":
                            const matchingElasticSearchTask = this.ravenEtlTasks().find(x => x.taskName() === taskProgress.TaskName);
                            if (matchingElasticSearchTask) {
                                matchingElasticSearchTask.updateProgress(taskProgress);
                            }
                            break;
                    }
                });

                // tasks w/o defined connection string won't get progress update - update them manually 

                this.sqlEtlTasks().forEach(task => {
                    if (task.loadingProgress()) {
                        task.loadingProgress(false);
                    }
                });

                this.olapEtlTasks().forEach(task => {
                    if (task.loadingProgress()) {
                        task.loadingProgress(false);
                    }
                });

                this.ravenEtlTasks().forEach(task => {
                    if (task.loadingProgress()) {
                        task.loadingProgress(false);
                    }
                });

                this.elasticSearchEtlTasks().forEach(task => {
                    if (task.loadingProgress()) {
                        task.loadingProgress(false);
                    }
                });
            });
    }

   
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

    private fetchOngoingTasks(): JQueryPromise<Raven.Server.Web.System.OngoingTasksResult> {
        const db = this.db;
        return new ongoingTasksCommand(db)
            .execute()
            .done((info) => {

                console.warn("USING TEMPORARY VALUES");
                info.OngoingTasksList.forEach(task => {
                    task.ResponsibleNode = { //TODO: temp!
                        NodeTag: "A",
                        NodeUrl: window.location.hostname,
                        ResponsibleNode: "A"
                    }

                    if (task.TaskType === "OlapEtl") {
                        (task as any).Destination = "TODO - temporary description";
                    }
                })
                this.processTasksResult(info);
            });
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

    private watchEtlProgress() {
        if (!this.etlProgressWatch) {
            this.fetchEtlProcess();

            let intervalId = setInterval(() => {
                this.fetchEtlProcess();
            }, 3000);

            this.etlProgressWatch = intervalId;

            this.registerDisposable({
                dispose: () => {
                    if (intervalId) {
                        clearInterval(intervalId);
                        intervalId = null;
                        this.etlProgressWatch = null;
                    }
                }
            })
        }
    }

    toggleDetails(item: ongoingTaskListModel) {
        item.toggleDetails();

        const isEtl = item.taskType() === "RavenEtl" || item.taskType() === "SqlEtl" || item.taskType() === "OlapEtl" || item.taskType() === "ElasticSearchEtl";
        if (item.showDetails() && isEtl) {
            this.watchEtlProgress();
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

    showItemPreview(item: ongoingTaskListModel, scriptName: string) {
        //const type: Raven.Client.Documents.Operations.ETL.EtlType = item.taskType() === "RavenEtl" ? "Raven" : item.taskType() === "SqlEtl" ? "Sql" : "Olap";
        let type:Raven.Client.Documents.Operations.ETL.EtlType;

        switch (item.taskType()) {
            case "RavenEtl": type = "Raven"; break;
            case "SqlEtl": type = "Sql"; break;
            case "OlapEtl": type = "Olap"; break;
            case "ElasticSearchEtl": type = "ElasticSearch"; break;
        }

        this.definitionsCache.showDefinitionFor(type, item.taskId, scriptName);
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

<script type="text/html" id="etl-progress-template">
    <div data-bind="visible: suggestNavigationToResponsibleNodeForProgress">
        <small><i class="icon-info"></i> Navigate to <a target="_blank" href="#" data-bind="attr: { href: $root.createResponsibleNodeUrl($data) }">responsible node</a> to see task progress.</small>
    </div>
    <div data-bind="if: showProgress">
        <div data-bind="visible: loadingProgress">
            <i class="global-spinner spinner-xs"></i> Loading progress ...
        </div>
        <!-- ko foreach: scriptProgress -->
        <div class="etl-progress">
            <div class="overall-container">
                <div class="text-info flex-horizontal">
                    <span data-bind="text: name"></span>
                    <a title="Show script preview" href="#" data-bind="click: _.partial($root.showItemPreview, $parent, name())"><i class="icon-preview margin-left margin-left-xs"></i></a>
                    <div class="flex-separator"></div>
                    <div class="text-warning" data-bind="visible: !globalProgress.total()">
                        <small>
                            <i class="icon-warning"></i>
                            This script doesn't match any documents.
                        </small>
                    </div>
                </div>
                <div class="flex-grow"></div>
                <div class="progress-overall">
                    <div class="flex-horizontal" data-bind="with: globalProgress">
                        <div class="flex-grow"><span data-bind="text: formattedTimeLeftToProcess"></span></div>
                        <div class="percentage" data-bind="text: percentageFormatted,  css: { 'text-success': completed }, attr: { title: textualProgress }"></div>
                    </div>
                    <div class="progress" data-bind="with: globalProgress">
                        <div data-bind="css: { 'progress-bar-striped': !completed() && !disabled(), 'progress-bar-primary': !completed(), 'active': !completed() && !disabled(), 'progress-bar-success': completed },
                                        style: { width: percentageFormatted }, attr: { 'aria-valuenow': percentage, title: textualProgress }"
                             class="progress-bar" role="progressbar" aria-valuemin="0" aria-valuemax="100">
                            <span class="sr-only" data-bind="text: percentageFormatted() + ' Completed'"></span>
                        </div>
                    </div>
                </div>f
            </div>
            <div data-bind="foreach: innerProgresses" class="etl-progress-details">
                <div class="etl-progress-item" data-bind="with: progress, visible: visible">
                    <div class="etl-label">
                        <small class="name" data-bind="text: $parent.name"></small>
                        <small class="percentage" data-bind="text: percentageFormatted(), css: { 'text-success': completed }, attr: { title: textualProgress }"></small>
                    </div>
                    <div class="progress">
                        <div data-bind="css: { 'progress-bar-striped': !completed() && !$parents[1].globalProgress.disabled(), 'progress-bar-primary': !completed(), 'active': !completed() && !$parents[1].globalProgress.disabled(), 'progress-bar-success': completed() },
                                        style: { width: percentageFormatted }, attr: { 'aria-valuenow': percentage, title: textualProgress }"
                             class="progress-bar" role="progressbar" aria-valuemin="0" aria-valuemax="100">
                            <span class="sr-only" data-bind="text: percentageFormatted() + ' Completed'"></span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <!-- /ko -->
    </div>
</script>



//TODO: remove
// this class represents connection between current node (hub) and remote node (sink)
class ongoingTaskReplicationHubListModel extends ongoingTaskListModel {
    
    uniqueName: string;
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub) {
        super();

        this.update(dto); 
        this.initializeObservables();
    }
    
    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub) {
        super.update(dto);

        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl || "N/A");
        
        this.uniqueName = ongoingTaskReplicationHubListModel.generateUniqueName(dto);
    }
    
    toggleDetails(): void {
        throw new Error("Use toggleDetails on pullReplicationHub definition level");
    }
    
    static generateUniqueName(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub) { 
        return dto.TaskName + ":" + dto.DestinationDatabase + ":" + dto.DestinationUrl;
    }
}

type PerConnectionStats = {
    clientUri: string;
    workerId: string;
    strategy?: Raven.Client.Documents.Subscriptions.SubscriptionOpeningStrategy;
}

//TODO: remove
class ongoingTaskSubscriptionListModel extends ongoingTaskListModel {
    
    activeDatabase = activeDatabaseTracker.default.database;

    // General stats
    lastTimeServerMadeProgressWithDocuments = ko.observable<string>();
    lastClientConnectionTime = ko.observable<string>();
    changeVectorForNextBatchStartingPoint = ko.observable<string>(null);

    // Live connection stats
    clients = ko.observableArray<PerConnectionStats>([]);
    clientDetailsIssue = ko.observable<string>(); // null (ok) | client is not connected | failed to get details..
    subscriptionMode = ko.observable<string>();
    textClass = ko.observable<string>("text-details");

    validationGroup: KnockoutValidationGroup;
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription) {
        super();

        this.update(dto);
        this.initializeObservables(); 
        
        _.bindAll(this, "disconnectClientFromSubscription");
    }

    initializeObservables(): void {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editSubscription(this.taskId, this.taskName());

        this.taskState.subscribe(() => this.refreshIfNeeded());
    }

    toggleDetails(): void {
        this.showDetails.toggle();
        this.refreshIfNeeded()
    }
    
    private refreshIfNeeded(): void {
        if (this.showDetails()) {
            this.refreshSubscriptionInfo();
        }
    }

    private refreshSubscriptionInfo() {
        // 1. Get general info
        ongoingTaskInfoCommand.forSubscription(this.activeDatabase(), this.taskId, this.taskName())
            .execute()
            .done((result: Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails) => {

                this.responsibleNode(result.ResponsibleNode);
                this.taskState(result.Disabled ? 'Disabled' : 'Enabled');
                
                this.changeVectorForNextBatchStartingPoint(result.ChangeVectorForNextBatchStartingPoint);
                
                const dateFormat = generalUtils.dateFormat;

                const lastServerTime = (!!result.LastBatchAckTime) ? moment.utc(result.LastBatchAckTime).local().format(dateFormat):"N/A";
                this.lastTimeServerMadeProgressWithDocuments(lastServerTime);
                const lastClientTime = (!!result.LastClientConnectionTime)?moment.utc(result.LastClientConnectionTime).local().format(dateFormat):"N/A";
                this.lastClientConnectionTime(lastClientTime);

                // 2. Get connection details info
                this.clientDetailsIssue(null);
                new subscriptionConnectionDetailsCommand(this.activeDatabase(), this.taskId, this.taskName(), this.responsibleNode().NodeUrl)
                    .execute()
                    .done((result: Raven.Server.Documents.TcpHandlers.SubscriptionConnectionsDetails) => {

                        this.subscriptionMode(result.SubscriptionMode);
                        
                        this.clients(result.Results.map(x => ({
                            clientUri: x.ClientUri,
                            strategy: x.Strategy,
                            workerId: x.WorkerId
                        })));

                        if (!result.Results.length) { 
                            this.clientDetailsIssue("No client is connected");
                            this.textClass("text-warning");
                        }
                    })
                    .fail((response: JQueryXHR) => {
                        if (response.status === 0) {
                            // we can't even connect to node, show node connectivity error
                            this.clientDetailsIssue("Failed to connect to " + this.responsibleNode().NodeUrl + ". Please make sure this url is accessible from your browser.");
                        } else {
                            this.clientDetailsIssue("Failed to get client connection details");
                        }
                        
                        this.textClass("text-danger");
                    });
            });
    }

    disconnectClientFromSubscription(workerId: string) {
        new dropSubscriptionConnectionCommand(this.activeDatabase(), this.taskId, this.taskName(), workerId)
            .execute()
            .done(() => this.refreshSubscriptionInfo());
    }
}

export = ongoingTaskSubscriptionListModel;


//TODO remove


class progressItem {
    name = ko.observable<string>();

    globalProgress: etlProgress = new etlProgress(0, 0, x => x.toLocaleString());

    documents = new genericProgress(0, 0, x => x.toLocaleString());
    documentTombstones = new genericProgress(0, 0, x => x.toLocaleString());
    counterGroups = new genericProgress(0, 0, x => x.toLocaleString());
    
    countersVisible = ko.pureComputed(() => {
        const countersCount = this.counterGroups.total();
        return countersCount > 0;
    });

    innerProgresses = [
        { name: "Documents", progress: this.documents, visible: true },
        { name: "Document Tombstones", progress: this.documentTombstones, visible: true },
        { name: "Counter Groups", progress: this.counterGroups, visible: this.countersVisible }
    ]; 
    
    constructor(dto: Raven.Server.Documents.ETL.Stats.EtlProcessProgress) {
        this.update(dto);
    }
    
    update(dto: Raven.Server.Documents.ETL.Stats.EtlProcessProgress) {
        this.name(dto.TransformationName);
        
        this.documents.total(dto.TotalNumberOfDocuments);
        this.documents.processed(dto.TotalNumberOfDocuments - dto.NumberOfDocumentsToProcess);
        
        this.documentTombstones.total(dto.TotalNumberOfDocumentTombstones);
        this.documentTombstones.processed(dto.TotalNumberOfDocumentTombstones - dto.NumberOfDocumentTombstonesToProcess);
        
        this.counterGroups.total(dto.TotalNumberOfCounterGroups);
        this.counterGroups.processed(dto.TotalNumberOfCounterGroups - dto.NumberOfCounterGroupsToProcess);
        
        this.globalProgress.processedPerSecond(dto.AverageProcessedPerSecond);
        this.globalProgress.total(
            this.documents.total() +
            this.documentTombstones.total() +
            this.counterGroups.total()
        );
        
        this.globalProgress.completed(dto.Completed);
        this.globalProgress.disabled(dto.Disabled);
        
        this.globalProgress.processed(
            this.documents.processed() +
            this.documentTombstones.processed() +
            this.counterGroups.processed()
        );
    }
}

//TODO: remove
abstract class abstractOngoingTaskEtlListModel extends ongoingTaskListModel {
    showProgress = ko.observable(false); // we use separate property for progress and details to smooth toggle animation, first we show progress then expand details 
    
    loadingProgress = ko.observable<boolean>(true);

    canShowProgress = ko.pureComputed(() => {
        const status = this.taskConnectionStatus();
        return status === "Active" || status === "NotActive" || status === "Reconnect";
    });
    
    suggestNavigationToResponsibleNodeForProgress = ko.pureComputed(() => this.taskConnectionStatus() === "NotOnThisNode");
    
    connectionStringsUrl: string;
    connectionStringName = ko.observable<string>();
    
    scriptProgress = ko.observableArray<progressItem>([]);

    toggleDetails() {
        this.showProgress(!this.showDetails() && this.canShowProgress());
        this.showDetails.toggle();
    }
    
    updateProgress(incomingProgress: Raven.Server.Documents.ETL.Stats.EtlTaskProgress) {
        const existingNames = this.scriptProgress().map(x => x.name());
        
        incomingProgress.ProcessesProgress.forEach(incomingScriptProgress => {
            const existingItem = this.scriptProgress().find(x => x.name() === incomingScriptProgress.TransformationName);
            if (existingItem) {
                existingItem.update(incomingScriptProgress);
                _.pull(existingNames, incomingScriptProgress.TransformationName);
            } else {
                this.scriptProgress.push(new progressItem(incomingScriptProgress));
            }
        });
        
        if (existingNames.length) {
            // remove those scripts
            existingNames.forEach(toDelete => {
                const item = this.scriptProgress().find(x => x.name() === toDelete);
                this.scriptProgress.remove(item);
            })
        }
        
        this.loadingProgress(false);
    }
}

export = abstractOngoingTaskEtlListModel;



class etlProgress extends genericProgress {
    
    disabled = ko.observable<boolean>(false);
    
    constructor(processed: number,
                total: number,
                numberFormatter: (number: number) => string,
                processedPerSecond: number = 0) {
        super(processed, total, numberFormatter, processedPerSecond);
        
        this.completed = ko.observable<boolean>(false); //override property - here we have explicit complete 
        
        this.percentage = ko.pureComputed(() => {
            const percentage = this.defaultPercentage();
            return percentage === 100 && !this.completed() ? 99.9 : percentage;
        });
        
        this.formattedTimeLeftToProcess = ko.pureComputed(() => {
            if (this.disabled()) {
                return "Overall progress";
            }
            if (this.completed()) {
                return "ETL completed";
            }
            return this.defaultFormattedTimeLeftToProcess();
        });
        
        this.textualProgress = ko.pureComputed(() => {
            if (this.total() === this.processed() && !this.completed()) {
                return "Processed all documents and tombstones, load in progress";
            }
            return this.defaultTextualProgress();
        })
    }

    protected getDefaultTimeLeftMessage() {
        if (this.total() === this.processed() && !this.completed()) {
            return "Processed all documents and tombstones, load in progress";
        }

        return this.disabled() ? `Task is disabled` : "Overall progress";
    }
}


export = etlProgress; 



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



*/
