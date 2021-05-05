import viewModelBase = require("viewmodels/viewModelBase");
import index = require("models/database/index/index");
import appUrl = require("common/appUrl");
import saveIndexLockModeCommand = require("commands/database/index/saveIndexLockModeCommand");
import app = require("durandal/app");
import deleteIndexesConfirm = require("viewmodels/database/indexes/deleteIndexesConfirm");
import forceIndexReplace = require("commands/database/index/forceIndexReplace");
import saveIndexPriorityCommand = require("commands/database/index/saveIndexPriorityCommand");
import getIndexesStatsCommand = require("commands/database/index/getIndexesStatsCommand");
import getIndexesStatusCommand = require("commands/database/index/getIndexesStatusCommand");
import resetIndexCommand = require("commands/database/index/resetIndexCommand");
import openFaultyIndexCommand = require("commands/database/index/openFaultyIndexCommand");
import togglePauseIndexingCommand = require("commands/database/index/togglePauseIndexingCommand");
import eventsCollector = require("common/eventsCollector");
import enableIndexCommand = require("commands/database/index/enableIndexCommand");
import disableIndexCommand = require("commands/database/index/disableIndexCommand");
import getIndexesProgressCommand = require("commands/database/index/getIndexesProgressCommand");
import indexProgress = require("models/database/index/indexProgress");
import indexStalenessReasons = require("viewmodels/database/indexes/indexStalenessReasons");
import generalUtils = require("common/generalUtils");
import shell = require("viewmodels/shell");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import bulkIndexOperationConfirm = require("./bulkIndexOperationConfirm");

type indexGroup = {
    entityName: string;
    indexes: KnockoutObservableArray<index>;
    groupHidden: KnockoutObservable<boolean>;
};

class indexes extends viewModelBase {

    indexGroups = ko.observableArray<indexGroup>();
    sortedGroups: KnockoutComputed<indexGroup[]>;
    newIndexUrl = appUrl.forCurrentDatabase().newIndex;
    searchText = ko.observable<string>("");
    hasAnyStateFilter: KnockoutComputed<boolean>;
    autoRefresh = ko.observable<boolean>(true);
    showOnlyIndexesWithIndexingErrors = ko.observable<boolean>(false);
    indexStatusFilter = ko.observableArray<indexStatus>(["Normal", "ErrorOrFaulty", "Stale", "Paused", "Disabled", "Idle", "RollingDeployment"]);
    lockModeCommon: KnockoutComputed<string>;
    selectedIndexesName = ko.observableArray<string>();
    indexesSelectionState: KnockoutComputed<checkbox>;
    indexProgressInterval: number;
    indexingProgresses = new Map<string, indexProgress>();
    requestedIndexingInProgress = false;
    indexesCount: KnockoutComputed<number>;
    searchCriteriaDescription: KnockoutComputed<string>;

    private clusterManager = clusterTopologyManager.default;
    localNodeTag = ko.observable<string>();
    isCluster: KnockoutComputed<boolean>;

    spinners = {
        globalStartStop: ko.observable<boolean>(false),
        globalLockChanges: ko.observable<boolean>(false),
        localPriority: ko.observableArray<string>([]),
        localLockChanges: ko.observableArray<string>([]),
        localState: ko.observableArray<string>([]),
        swapNow: ko.observableArray<string>([])
    };

    globalIndexingStatus = ko.observable<Raven.Client.Documents.Indexes.IndexRunningStatus>();

    resetsInProgress = new Set<string>();

    throttledRefresh: Function;
    indexesProgressRefreshThrottle: Function;

    indexErrorsUrl = ko.pureComputed(() => appUrl.forIndexErrors(this.activeDatabase()));

    constructor() {
        super();
        this.initObservables();
        this.bindToCurrentInstance(
            "lowPriority", "highPriority", "normalPriority",
            "openFaultyIndex", "resetIndex", "deleteIndex",
            "forceSideBySide",
            "showStaleReasons",
            "unlockIndex", "lockIndex", "lockErrorIndex",
            "enableIndex", "disableIndex", "disableSelectedIndexes", "enableSelectedIndexes",
            "pauseSelectedIndexes", "resumeSelectedIndexes",
            "unlockSelectedIndexes", "lockSelectedIndexes", "lockErrorSelectedIndexes",
            "deleteSelectedIndexes", "startIndexing", "stopIndexing", "resumeIndexing", "pauseUntilRestart", "toggleSelectAll"
        );

        // refresh not often then 5 seconds, but delay refresh by 2 seconds
        this.throttledRefresh = _.throttle(() => setTimeout(() => this.fetchIndexes(), 2000), 5000);

        // refresh every 3 seconds
        this.indexesProgressRefreshThrottle = _.throttle(() => this.getIndexesProgress(), 3000);
        this.indexProgressInterval = setInterval(() => {
            const indexes = this.getAllIndexes();
            if (indexes.length === 0) {
                return;
            }

            const anyIndexStale = indexes.find(x => x.isStale() && !x.isDisabledState());
            const anyRollingDeployment = indexes.find(x => x.rollingDeploymentInProgress() && !x.isDisabledState());

            if (anyIndexStale || anyRollingDeployment) {
                this.indexesProgressRefreshThrottle();
            }
        }, 3000);
        
        this.indexesCount = ko.pureComputed(() => {
            return _.sum(this.indexGroups().map(x => x.indexes().length));
        });
        
        this.searchCriteriaDescription = ko.pureComputed(() => {
            let indexesCount = 0;
            let totalProcessedPerSecond = 0;

            this.indexGroups().forEach(indexGroup => {
                const indexesInGroup = indexGroup.indexes().filter(i => !i.filteredOut());
                indexesCount += indexesInGroup.length;

                totalProcessedPerSecond += _.sum(indexesInGroup
                    .filter(i => i.progress() || (i.replacement() && i.replacement().progress()))
                    .map(i => {
                        let sum = 0;

                        const progress = i.progress();
                        if (progress) {
                            sum += progress.globalProgress().processedPerSecond();
                        }

                        const replacement = i.replacement();
                        if (replacement) {
                            const replacementProgress = replacement.progress();
                            if (replacementProgress) {
                                sum += replacementProgress.globalProgress().processedPerSecond();
                            }
                        }

                        return sum;
                    }));
            });
                        
            if (!this.indexStatusFilter().length) {
                return `All <strong>Index Status</strong> options are unchecked. Please select options under <strong>'Index Status'</strong> to view indexes list.`;
            }
            
            const indexingErrorsOnlyPart = this.showOnlyIndexesWithIndexingErrors() ? `, with <strong>indexing errors only</strong>,` : "";
            
            const firstPart = indexesCount
                ? `Displaying <strong>${indexesCount}</strong> ${this.pluralize(indexesCount, "index", "indexes", true)}${indexingErrorsOnlyPart} that match Status Filter:`
                : "No matching indexes for Status Filter:";
           
            const statusFilterPart = `<strong>${this.indexStatusFilter().map(x => this.mapIndexStatus(x)).join(", ")}</strong> `;
            
            const namePart = this.searchText() ? `, where name contains <strong>${generalUtils.escapeHtml(this.searchText())}</strong>` : "";

            const refreshPart = `Auto refresh is <strong>${this.autoRefresh() ? "on" : "off"}</strong>`;
            
            const speedPart = `Processing Speed: <strong>${Math.floor(totalProcessedPerSecond).toLocaleString()}</strong> docs / sec`;
            
            return `${firstPart} ${statusFilterPart} ${namePart}. ${refreshPart}. ${speedPart}`;
        });
    }
    
    private mapIndexStatus(status: indexStatus) {
        switch (status) {
            case "Normal":
            case "Disabled":
            case "Idle":
            case "Paused":
            case "Stale":
                return status;
            case "RollingDeployment":
                return "Rolling deployment";
            case "ErrorOrFaulty":
                return "Error, Faulty";
        }
    }

    private getAllIndexes(): index[] {
        const all: index[] = [];
        this.indexGroups().forEach(g => all.push(...g.indexes()));

        all.forEach(idx => {
            if (idx.replacement()) {
                all.push(idx.replacement());
            }
        });

        return _.uniq(all);
    }

    private getSelectedIndexes(): Array<index> {
        const selectedIndexes = this.selectedIndexesName();
        return this.getAllIndexes().filter(x => _.includes(selectedIndexes, x.name));
    }

    private initObservables() {
        this.localNodeTag = this.clusterManager.localNodeTag;
        this.isCluster = ko.pureComputed(() => this.clusterManager.nodesCount() > 1);
        
        this.searchText.throttle(200).subscribe(() => this.filterIndexes(false));
        this.indexStatusFilter.subscribe(() => this.filterIndexes(false));
        this.showOnlyIndexesWithIndexingErrors.subscribe(() => this.filterIndexes(false));
        this.autoRefresh.subscribe(refresh => {
            if (refresh) {
                this.filterIndexes(false);
                this.fetchIndexes(true);
            }
        });
        
        this.hasAnyStateFilter = ko.pureComputed(() => {
            const autoRefresh = this.autoRefresh();
            const filterCount = this.indexStatusFilter();
            const withIndexingErrorsOnly = this.showOnlyIndexesWithIndexingErrors();
           
            return !autoRefresh || filterCount.length !== 6 || withIndexingErrorsOnly;
        });

        this.sortedGroups = ko.pureComputed<indexGroup[]>(() => {
            const groups = this.indexGroups().slice(0).sort((l, r) => generalUtils.sortAlphaNumeric(l.entityName, r.entityName));

            groups.forEach((group: { entityName: string; indexes: KnockoutObservableArray<index> }) => {
                group.indexes(group.indexes().slice(0).sort((l, r) => generalUtils.sortAlphaNumeric(l.name, r.name)));
            });

            return groups;
        });

        this.lockModeCommon = ko.pureComputed(() => {
            const selectedIndexes = this.getSelectedIndexes();
            if (selectedIndexes.length === 0)
                return "None";

            const firstLockMode = selectedIndexes[0].lockMode();
            for (let i = 1; i < selectedIndexes.length; i++) {
                if (selectedIndexes[i].lockMode() !== firstLockMode) {
                    return "Mixed";
                }
            }
            return firstLockMode;
        });
        this.indexesSelectionState = ko.pureComputed<checkbox>(() => {
            const selectedCount = this.selectedIndexesName().length;
            const indexesCount = this.getAllIndexes().length;
            if (indexesCount && selectedCount === indexesCount)
                return "checked";
            if (selectedCount > 0)
                return "some_checked";
            return "unchecked";
        });
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('AIHAR1');
        
        if (args && args.stale) {
            this.indexStatusFilter(["Stale"]);
        }

        return this.fetchIndexes();
    }

    deactivate() {
        super.deactivate();

        clearInterval(this.indexProgressInterval);
    }
    
    compositionComplete() {
        super.compositionComplete();

        $('.index-info [data-toggle="tooltip"]').tooltip();
    }
    
    private fetchIndexes(forceRefresh: boolean = false): JQueryPromise<void> {
        if (!this.autoRefresh()) {
            return;
        }
        
        const statsTask = new getIndexesStatsCommand(this.activeDatabase())
            .execute();

        const statusTask = new getIndexesStatusCommand(this.activeDatabase())
            .execute();

        return $.when<any>(statsTask, statusTask)
            .done(([stats]: [Array<Raven.Client.Documents.Indexes.IndexStats>], [statuses]: [Raven.Client.Documents.Indexes.IndexingStatus]) => this.processData(stats, statuses, forceRefresh));
    }

    private processData(stats: Array<Raven.Client.Documents.Indexes.IndexStats>, statuses: Raven.Client.Documents.Indexes.IndexingStatus, forceRefresh: boolean) {                 
        if (forceRefresh) {
            this.indexGroups([]);
        }
        
        this.globalIndexingStatus(statuses.Status);

        const replacements = stats
            .filter(i => i.Name.startsWith(index.SideBySideIndexPrefix));

        stats
            .filter(i => !i.Name.startsWith(index.SideBySideIndexPrefix))
            .map(i => new index(i, this.globalIndexingStatus ))
            .forEach(idx => {
                this.putIndexIntoGroups(idx);

                if (this.indexingProgresses.get(idx.name)) {
                    return;
                }

                this.indexesProgressRefreshThrottle();
            });

        this.filterIndexes(true);
        
        this.processReplacements(replacements);
        this.syncIndexingProgress();
    }

    private processReplacements(replacements: Raven.Client.Documents.Indexes.IndexStats[]) {
        const replacementCache = new Map<string, Raven.Client.Documents.Indexes.IndexStats>();

        replacements.forEach(item => {
            const forIndex = item.Name.substr(index.SideBySideIndexPrefix.length);
            replacementCache.set(forIndex, item);

            if (this.indexingProgresses.get(item.Name)) {
                return;
            }

            this.indexesProgressRefreshThrottle();
        });

        this.indexGroups().forEach(group => {
            group.indexes().forEach(indexDef => {
                const replacementDto = replacementCache.get(indexDef.name);
                if (replacementDto) {
                    indexDef.replacement(new index(replacementDto, this.globalIndexingStatus, indexDef));
                } else {
                    indexDef.replacement(null);
                }
            });
        });
    }

    private putIndexIntoGroups(i: index): void {
        const targetGroupName = i.getGroupName();
        
        // group name might change so clear all items in different groups 
        // i.e. Faulty Index has group 'Other' but after refresh it goes to valid group 
        
        this.removeIndexesFromAllGroups(this.findIndexesByName(i.name), targetGroupName);
        
        this.putIndexIntoGroupNamed(i, targetGroupName);
    }

    private putIndexIntoGroupNamed(i: index, groupName: string): void {
        const group = this.indexGroups().find(g => g.entityName === groupName);
        if (group) {
            const oldIndex = group.indexes().find((cur: index) => cur.name === i.name);
            if (oldIndex) {
                oldIndex.updateWith(i);
            } else {
                group.indexes.push(i);
            }
        } else {
            this.indexGroups.push({
                entityName: groupName,
                indexes: ko.observableArray([i]),
                groupHidden: ko.observable<boolean>(false)
            });
        }
    }

    private filterIndexes(passive: boolean) {
        if (passive && !this.autoRefresh()) {
            // do NOT touch visibility of indexes!
            return;
        }
        
        const filterLower = this.searchText().toLowerCase();
        const typeFilter = this.indexStatusFilter();
        const withIndexingErrorsOnly = this.showOnlyIndexesWithIndexingErrors();
        
        const selectedIndexes = this.selectedIndexesName();
        let selectionChanged = false;
        
        this.indexGroups().forEach(indexGroup => {
            let hasAnyInGroup = false;
            indexGroup.indexes().forEach(index => {
                const match = index.filter(filterLower, typeFilter, withIndexingErrorsOnly);
                if (match) {
                    hasAnyInGroup = true;
                } else if (_.includes(selectedIndexes, index.name)) {
                    _.pull(selectedIndexes, index.name);
                    selectionChanged = true;
                }
            });

            indexGroup.groupHidden(!hasAnyInGroup);
        });
        
        if (selectionChanged) {
            this.selectedIndexesName(selectedIndexes);    
        }
    }

    createIndexesUrlObservableForNode(nodeTag: string) {
        return ko.pureComputed(() => {
            const nodeInfo = this.clusterManager.getClusterNodeByTag(nodeTag);
            const link = appUrl.forIndexes(this.activeDatabase());
            return appUrl.toExternalUrl(nodeInfo.serverUrl(), link);
        });
    }
    
    private getIndexesProgress() {
        if (!this.autoRefresh()) {
            return;
        }
        
        if (shell.showConnectionLost()) {
            // looks like we don't have connection to server, skip index progress update 
            return $.Deferred().fail();
        }

        if (this.requestedIndexingInProgress) {
            return $.Deferred().resolve();
        }

        this.requestedIndexingInProgress = true;

        return new getIndexesProgressCommand(this.activeDatabase())
            .execute()
            .done(indexesProgressList => {
                const progressToProcess = Array.from(this.indexingProgresses.keys());

                for (let i = 0; i < indexesProgressList.length; i++) {
                    const dto = indexesProgressList[i];
                    if (this.indexingProgresses.has(dto.Name)) {
                        this.indexingProgresses.get(dto.Name).updateProgress(new indexProgress(dto));
                    } else {
                        this.indexingProgresses.set(dto.Name, new indexProgress(dto));    
                    }
                    
                    _.pull(progressToProcess, dto.Name);
                }

                // progressToProcess contains now non-stale indexes
                // set progress to 100%

                progressToProcess.forEach(name => {
                    const progress = this.indexingProgresses.get(name);
                    progress.markCompleted();
                });

                this.syncIndexingProgress();
            })
            .always(() => this.requestedIndexingInProgress = false);
    }
    
    /* passes indexing progress to index instance */
    private syncIndexingProgress() {
        this.indexGroups().forEach(group => {
            group.indexes().forEach(indexDef => {
                const progress = this.indexingProgresses.get(indexDef.name);
                if (progress !== indexDef.progress()) {
                    indexDef.progress(progress);
                }

                const indexReplacement = indexDef.replacement();
                if (indexReplacement) {
                    const replacementProgress = this.indexingProgresses.get(indexReplacement.name);
                    if (replacementProgress !== indexReplacement.progress()) {
                        indexReplacement.progress(replacementProgress);
                    }
                }
            });
        });
    }

    openFaultyIndex(i: index) {
        this.confirmationMessage("Open index?", `You're opening a faulty index <strong>'${generalUtils.escapeHtml(i.name)}'</strong>`, {
            html: true
        })
            .done(result => {
                if (result.can) {

                    eventsCollector.default.reportEvent("indexes", "open");

                    new openFaultyIndexCommand(i.name, this.activeDatabase())
                        .execute();
                }
            });
    }

    resetIndex(i: index) {
        this.confirmationMessage("Reset index?", `You're resetting <strong>'${generalUtils.escapeHtml(i.name)}'</strong>`, {
            html: true
        })
            .done(result => {
                if (result.can) {

                    eventsCollector.default.reportEvent("indexes", "reset");

                    // reset index is implemented as delete and insert, so we receive notification about deleted index via changes API
                    // let's issue marker to ignore index delete information for next few seconds because it might be caused by reset.
                    // Unfortunately we can't use resetIndexVm.resetTask.done, because we receive event via changes api before resetTask promise 
                    // is resolved. 
                    this.resetsInProgress.add(i.name);

                    new resetIndexCommand(i.name, this.activeDatabase())
                        .execute();

                    setTimeout(() => {
                        this.resetsInProgress.delete(i.name);
                    }, 30000);
                }
            });
    }

    deleteIndex(i: index) {
        eventsCollector.default.reportEvent("indexes", "delete");
        this.promptDeleteIndexes([i]);
        this.resetsInProgress.delete(i.name);
    }

    private processIndexEvent(e: Raven.Client.Documents.Changes.IndexChange) {
        if (!this.autoRefresh()) {
            return;    
        }
        
        switch (e.Type) {
            case "IndexRemoved":
                if (!this.resetsInProgress.has(e.Name)) {
                    this.removeIndexesFromAllGroups(this.findIndexesByName(e.Name));
                    this.removeSideBySideIndexesFromAllGroups(e.Name);
                }
                break;
            case "BatchCompleted":
                if (this.indexingProgresses.get(e.Name)) {
                    this.indexesProgressRefreshThrottle();
                }
                break;
            case "SideBySideReplace":
                const sideBySideIndexName = index.SideBySideIndexPrefix + e.Name;
                this.removeSideBySideIndexesFromAllGroups(sideBySideIndexName);
                break;
        }

        this.throttledRefresh();
    }

    private removeIndexesFromAllGroups(indexes: index[], skipGroup: string = null) {
        this.indexGroups()
            .filter(x => skipGroup ? x.entityName !== skipGroup : true)
            .forEach(g => {
                g.indexes.removeAll(indexes);
            });

        // Remove any empty groups.
        this.indexGroups.remove((item: indexGroup) => item.indexes().length === 0);
    }

    private removeSideBySideIndexesFromAllGroups(indexName: string) {
        this.indexGroups().forEach(g => {
            g.indexes().forEach(i => {
                const replacement = i.replacement();
                if (replacement && replacement.name === indexName) {
                    i.replacement(null);
                }
            });
        });
    }

    private findIndexesByName(indexName: string): index[] {
        const result = [] as Array<index>;
        this.indexGroups().forEach(g => {
            g.indexes().forEach(i => {
                if (i.name === indexName) {
                    result.push(i);
                }
            });
        });

        return result;
    }

    private promptDeleteIndexes(indexes: index[]) {
        if (indexes.length > 0) {
            const deleteIndexesVm = new deleteIndexesConfirm(indexes, this.activeDatabase());
            app.showBootstrapDialog(deleteIndexesVm); 
            deleteIndexesVm.deleteTask
                .done((deleted: boolean) => {
                    if (deleted) {
                        this.removeIndexesFromAllGroups(indexes);
                    }
                });
        }
    }

    unlockIndex(i: index) {
        eventsCollector.default.reportEvent("indexes", "set-lock-mode", "Unlock");
        this.updateIndexLockMode(i, "Unlock", "Unlocked");
    }

    lockIndex(i: index) {
        eventsCollector.default.reportEvent("indexes", "set-lock-mode", "LockedIgnore");
        this.updateIndexLockMode(i, "LockedIgnore", "Locked");
    }

    lockErrorIndex(i: index) {
        eventsCollector.default.reportEvent("indexes", "set-lock-mode", "LockedError");
        this.updateIndexLockMode(i, "LockedError","Locked (Error)");
    }

    private updateIndexLockMode(i: index, newLockMode: Raven.Client.Documents.Indexes.IndexLockMode, lockModeStrForTitle: string) {
        if (i.lockMode() !== newLockMode) {
            this.spinners.localLockChanges.push(i.name);

            new saveIndexLockModeCommand([i], newLockMode, this.activeDatabase(), lockModeStrForTitle)
                .execute()
                .done(() => i.lockMode(newLockMode))
                .always(() => this.spinners.localLockChanges.remove(i.name));
        }
    }

    normalPriority(idx: index) {
        eventsCollector.default.reportEvent("index", "priority", "normal");
        this.setIndexPriority(idx, "Normal");
    }

    lowPriority(idx: index) {
        eventsCollector.default.reportEvent("index", "priority", "low");
        this.setIndexPriority(idx, "Low");
    }

    highPriority(idx: index) {
        eventsCollector.default.reportEvent("index", "priority", "high");
        this.setIndexPriority(idx, "High");
    }

    private setIndexPriority(idx: index, newPriority: Raven.Client.Documents.Indexes.IndexPriority) {
        const originalPriority = idx.priority();
        if (originalPriority !== newPriority) {
            this.spinners.localPriority.push(idx.name);

            const optionalResumeTask = idx.globalIndexingStatus() === "Paused"
                ? this.resumeIndexingInternal(idx)
                : $.Deferred<void>().resolve();

            optionalResumeTask.done(() => {
                new saveIndexPriorityCommand(idx.name, newPriority, this.activeDatabase())
                    .execute()
                    .done(() => idx.priority(newPriority))
                    .always(() => this.spinners.localPriority.remove(idx.name));
            });
        }
    }

    protected afterClientApiConnected() {
        const changesApi = this.changesContext.databaseChangesApi();
        this.addNotification(changesApi.watchAllIndexes(e => this.processIndexEvent(e)));
    }

    forceSideBySide(idx: index) {
        this.confirmationMessage("Are you sure?", `Do you want to <strong>force swapping</strong> the side-by-side index: ${generalUtils.escapeHtml(idx.name)}?`, {
            html: true
        })
            .done((result: canActivateResultDto) => {
                if (result.can) {
                    this.spinners.swapNow.push(idx.name);
                    eventsCollector.default.reportEvent("index", "force-side-by-side");
                    new forceIndexReplace(idx.name, this.activeDatabase())
                        .execute()
                        .always(() => this.spinners.swapNow.remove(idx.name));
                }
            });
    }

    unlockSelectedIndexes() {
        this.setLockModeSelectedIndexes("Unlock", "Unlock");
    }

    lockSelectedIndexes() {
        this.setLockModeSelectedIndexes("LockedIgnore", "Lock");
    }

    lockErrorSelectedIndexes() {
        this.setLockModeSelectedIndexes("LockedError", "Lock (Error)");
    }

    private setLockModeSelectedIndexes(lockModeString: Raven.Client.Documents.Indexes.IndexLockMode, lockModeStrForTitle: string) {
        if (this.lockModeCommon() === lockModeString)
            return;

        this.confirmationMessage("Are you sure?", `Do you want to <strong>${generalUtils.escapeHtml(lockModeStrForTitle)}</strong> selected indexes?</br>Note: Static-indexes only will be set, 'Lock Mode' is not relevant for auto-indexes.`, {
            html: true
        })
            .done(can => {
                if (can) {
                    eventsCollector.default.reportEvent("index", "set-lock-mode-selected", lockModeString);
           
                    const indexes = this.getSelectedIndexes().filter(index => index.type() !== "AutoMap" && index.type() !== "AutoMapReduce");
                       
                    if (indexes.length) {
                        this.spinners.globalLockChanges(true);
                        
                        new saveIndexLockModeCommand(indexes, lockModeString, this.activeDatabase(), lockModeStrForTitle)
                            .execute()
                            .done(() => indexes.forEach(i => i.lockMode(lockModeString)))
                            .always(() => this.spinners.globalLockChanges(false));
                    }
                }
            });
    }

    disableSelectedIndexes(clusterWide: boolean) {
        this.toggleDisableSelectedIndexes(false, clusterWide);
    }

    enableSelectedIndexes(clusterWide: boolean) {
        this.toggleDisableSelectedIndexes(true, clusterWide);
    }

    private toggleDisableSelectedIndexes(enableIndex: boolean, clusterWide: boolean) {
        const selectedIndexes = this.getSelectedIndexes();
        const nodeTag = this.localNodeTag();

        const confirmation = clusterWide
            ? (enableIndex ? bulkIndexOperationConfirm.forClusterWideEnable(selectedIndexes) : bulkIndexOperationConfirm.forClusterWideDisable(selectedIndexes))
            : (enableIndex ? bulkIndexOperationConfirm.forEnable(selectedIndexes, nodeTag) : bulkIndexOperationConfirm.forDisable(selectedIndexes, nodeTag));
        
        confirmation.result
            .done(result => {
                if (result.can) {
                    eventsCollector.default.reportEvent("index", "toggle-status", status);

                    this.spinners.globalLockChanges(true);

                    selectedIndexes.forEach(i => enableIndex ? this.enableIndex(i, clusterWide) : this.disableIndex(i, clusterWide));
                }
            })
            .always(() => this.spinners.globalLockChanges(false));

        app.showBootstrapDialog(confirmation);
    }

    pauseSelectedIndexes() {
        this.togglePauseSelectedIndexes(false);
    }

    resumeSelectedIndexes() {
        this.togglePauseSelectedIndexes(true);
    }

    private togglePauseSelectedIndexes(resume: boolean) {
        const selectedIndexes = this.getSelectedIndexes();
        const nodeTag = this.localNodeTag();
        const confirmation = resume 
            ? bulkIndexOperationConfirm.forResume(selectedIndexes, nodeTag) 
            : bulkIndexOperationConfirm.forPause(selectedIndexes, nodeTag);
        
        confirmation.result
            .done(result => {
                if (result.can) {
                    eventsCollector.default.reportEvent("index", "toggle-status", status);

                    this.spinners.globalLockChanges(true);

                    selectedIndexes.forEach(i => resume ? this.resumeIndexing(i) : this.pauseUntilRestart(i));
                }
            })
            .always(() => this.spinners.globalLockChanges(false));

        app.showBootstrapDialog(confirmation);
    }

    deleteSelectedIndexes() {
        eventsCollector.default.reportEvent("indexes", "delete-selected");
        this.promptDeleteIndexes(this.getSelectedIndexes());
    }

    startIndexing(): void {
        this.confirmationMessage("Are you sure?", "Do you want to <strong>resume</strong> indexing?", {
            html: true
        })
            .done(result => {
                if (result.can) {
                    eventsCollector.default.reportEvent("indexes", "resume-all");
                    this.spinners.globalStartStop(true);
                    new togglePauseIndexingCommand(true, this.activeDatabase())
                        .execute()
                        .done(() => {
                            this.globalIndexingStatus("Running");
                        })
                        .always(() => {
                            this.spinners.globalStartStop(false);
                            this.fetchIndexes();
                        });
                }
            });
    }

    stopIndexing() {
        this.confirmationMessage("Are you sure?", "Do you want to <strong>pause indexing</strong> until server restart?", {
            html: true
        })
            .done(result => {
                if (result.can) {
                    eventsCollector.default.reportEvent("indexes", "pause-all");
                    this.spinners.globalStartStop(true);
                    new togglePauseIndexingCommand(false, this.activeDatabase())
                        .execute()
                        .done(() => {
                            this.globalIndexingStatus("Paused");
                        })
                        .always(() => {
                            this.spinners.globalStartStop(false);
                            this.fetchIndexes();
                        });
                }
            });
    }

    resumeIndexing(idx: index): JQueryPromise<void> {
        eventsCollector.default.reportEvent("indexes", "resume");
        if (idx.canBeResumed()) {
            this.spinners.localState.push(idx.name);

            return this.resumeIndexingInternal(idx)
                .done(() => {
                    idx.status("Running");
                })
                .always(() => this.spinners.localState.remove(idx.name));
        }
    }

    private resumeIndexingInternal(idx: index): JQueryPromise<void> {
        return new togglePauseIndexingCommand(true, this.activeDatabase(), { name: [idx.name] })
            .execute();
    }

    pauseUntilRestart(idx: index) {
        eventsCollector.default.reportEvent("indexes", "pause");
        if (idx.canBePaused()) {
            this.spinners.localState.push(idx.name);

            new togglePauseIndexingCommand(false, this.activeDatabase(), { name: [idx.name] })
                .execute().
                done(() => {
                    idx.status("Paused");
                })
                .always(() => this.spinners.localState.remove(idx.name));
        }
    }

    enableIndex(idx: index, enableClusterWide: boolean) {
        eventsCollector.default.reportEvent("indexes", "set-state", "enabled");

        if (idx.canBeEnabled() || enableClusterWide) {
            this.spinners.localState.push(idx.name);

            new enableIndexCommand(idx.name, this.activeDatabase(), enableClusterWide)
                .execute()
                .done(() => {
                    idx.state("Normal");
                    idx.status("Running");
                })
                .always(() => this.spinners.localState.remove(idx.name));
        }
    }

    disableIndex(idx: index, disableClusterWide: boolean) {
        eventsCollector.default.reportEvent("indexes", "set-state", "disabled");

        if (idx.canBeDisabled() || disableClusterWide) {
            this.spinners.localState.push(idx.name);

            new disableIndexCommand(idx.name, this.activeDatabase(), disableClusterWide)
                .execute()
                .done(() => {
                    idx.state("Disabled");
                    idx.status("Disabled");
                })
                .always(() => this.spinners.localState.remove(idx.name));
        }
    }

    toggleSelectAll() {
        eventsCollector.default.reportEvent("indexes", "toggle-select-all");
        const selectedIndexesCount = this.selectedIndexesName().length;

        if (selectedIndexesCount > 0) {
            this.selectedIndexesName([]);
        } else {
            const namesToSelect = [] as Array<string>;

            this.indexGroups().forEach(indexGroup => {
                if (!indexGroup.groupHidden()) {
                    indexGroup.indexes().forEach(index => {
                        if (!index.filteredOut() && !_.includes(namesToSelect, index.name)) {
                            namesToSelect.push(index.name);

                            if (index.replacement()) {
                                namesToSelect.push(index.replacement().name);
                            }
                        }
                    });
                }
            });
            this.selectedIndexesName(namesToSelect);
        }
    }

    showStaleReasons(idx: index) {
        const view = new indexStalenessReasons(this.activeDatabase(), idx.name);
        eventsCollector.default.reportEvent("indexes", "show-stale-reasons");
        app.showBootstrapDialog(view);
    }
}

export = indexes;
