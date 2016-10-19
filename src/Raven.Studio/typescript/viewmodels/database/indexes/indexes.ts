import viewModelBase = require("viewmodels/viewModelBase");
import index = require("models/database/index/index");
import appUrl = require("common/appUrl");
import saveIndexLockModeCommand = require("commands/database/index/saveIndexLockModeCommand");
import app = require("durandal/app");
import resetIndexConfirm = require("viewmodels/database/indexes/resetIndexConfirm");
import changeSubscription = require("common/changeSubscription");
import indexReplaceDocument = require("models/database/index/indexReplaceDocument");
import getPendingIndexReplacementsCommand = require("commands/database/index/getPendingIndexReplacementsCommand");
import cancelSideBySizeConfirm = require("viewmodels/database/indexes/cancelSideBySizeConfirm");
import deleteIndexesConfirm = require("viewmodels/database/indexes/deleteIndexesConfirm");
import forceIndexReplace = require("commands/database/index/forceIndexReplace");
import saveIndexPriorityCommand = require("commands/database/index/saveIndexPriorityCommand");
import getIndexesStatsCommand = require("commands/database/index/getIndexesStatsCommand");
import getIndexesStatusCommand = require("commands/database/index/getIndexesStatusCommand");
import toggleIndexingCommand = require("commands/database/index/toggleIndexingCommand");

type indexGroup = {
    entityName: string;
    indexes: KnockoutObservableArray<index>;
    groupHidden: KnockoutObservable<boolean>;
};

class indexes extends viewModelBase {

    indexGroups = ko.observableArray<indexGroup>();
    sortedGroups: KnockoutComputed<indexGroup[]>;
    newIndexUrl = appUrl.forCurrentDatabase().newIndex;
    searchText = ko.observable<string>();
    lockModeCommon: KnockoutComputed<string>;
    selectedIndexesName = ko.observableArray<string>();
    indexesSelectionState: KnockoutComputed<checkbox>;

    spinners = {
        globalStartStop: ko.observable<boolean>(false),
        globalLockChanges: ko.observable<boolean>(false),
        localPriority: ko.observableArray<string>([]),
        localLockChanges: ko.observableArray<string>([])
    }

    indexingEnabled = ko.observable<boolean>(true);

    resetsInProgress = new Set<string>();

    constructor() {
        super();
        this.initObservables();
    }

    private getAllIndexes(): index[] {
        const all: index[] = [];
        this.indexGroups().forEach(g => all.pushAll(g.indexes()));
        return all.distinct();
    }

    private getSelectedIndexes(): Array<index> {
        const selectedIndexes = this.selectedIndexesName();
        return this.getAllIndexes().filter(x => selectedIndexes.contains(x.name));
    }

    private initObservables() {
        this.searchText.throttle(200).subscribe(() => this.filterIndexes());

        this.sortedGroups = ko.computed<indexGroup[]>(() => {
            var groups = this.indexGroups().slice(0).sort((l, r) => l.entityName.toLowerCase() > r.entityName.toLowerCase() ? 1 : -1);

            groups.forEach((group: { entityName: string; indexes: KnockoutObservableArray<index> }) => {
                group.indexes(group.indexes().slice(0).sort((l: index, r: index) => l.name.toLowerCase() > r.name.toLowerCase() ? 1 : -1));
            });

            return groups;
        });

        this.lockModeCommon = ko.computed(() => {
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
                return checkbox.Checked;
            if (selectedCount > 0)
                return checkbox.SomeChecked;
            return checkbox.UnChecked;
        });
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('AIHAR1');

        return this.fetchIndexes();
    }

    attached() {
        super.attached();
        ko.postbox.publish("SetRawJSONUrl", appUrl.forIndexesRawData(this.activeDatabase())); //TODO: do we need it?
    }

    private fetchIndexes(): JQueryPromise<void> {
        const statsTask = new getIndexesStatsCommand(this.activeDatabase())
            .execute();

        const statusTask = new getIndexesStatusCommand(this.activeDatabase())
            .execute();

        const replacementTask = new getPendingIndexReplacementsCommand(this.activeDatabase()).execute(); //TODO: this is not working yet!

        return $.when<any>(statsTask, replacementTask, statusTask)
            .done(([stats]: [Array<Raven.Client.Data.Indexes.IndexStats>], [replacements]: [indexReplaceDocument[]], [statuses]: [Raven.Client.Data.Indexes.IndexingStatus]) => this.processData(stats, replacements, statuses));
    }

    processData(stats: Array<Raven.Client.Data.Indexes.IndexStats>, replacements: indexReplaceDocument[], statuses: Raven.Client.Data.Indexes.IndexingStatus) {
        //TODO: handle replacements

        const pausedStatus = "Paused" as Raven.Client.Data.Indexes.IndexRunningStatus;

        this.indexingEnabled(statuses.Status !== pausedStatus);

        stats
            .map(i => new index(i))
            .forEach(i => {
                const paused = !!statuses.Indexes.find(x => x.Name === i.name && x.Status === pausedStatus);
                i.pausedUntilRestart(paused);
                this.putIndexIntoGroups(i);
            });
                 
    }

    private putIndexIntoGroups(i: index): void {
        this.putIndexIntoGroupNamed(i, i.getGroupName());
    }

    private putIndexIntoGroupNamed(i: index, groupName: string): void {
        const group = this.indexGroups.first(g => g.entityName === groupName);
        if (group) {
            const oldIndex = group.indexes.first((cur: index) => cur.name === i.name);
            if (oldIndex) {
                group.indexes.replace(oldIndex, i);
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

    private filterIndexes() {
        const filterLower = this.searchText().toLowerCase();
        this.selectedIndexesName([]);
        this.indexGroups().forEach(indexGroup => {
            var hasAnyInGroup = false;
            indexGroup.indexes().forEach(index => {
                var match = index.name.toLowerCase().indexOf(filterLower) >= 0;
                index.filteredOut(!match);
                if (match) {
                    hasAnyInGroup = true;
                }
            });

            indexGroup.groupHidden(!hasAnyInGroup);
        });
    }

    resetIndex(indexToReset: index) {
        const resetIndexVm = new resetIndexConfirm(indexToReset.name, this.activeDatabase());

        // reset index is implemented as delete and insert, so we receive notification about deleted index via changes API
        // let's issue marker to ignore index delete information for next few seconds because it might be caused by reset.
        // Unfortunettely we can't use resetIndexVm.resetTask.done, because we receive event via changes api before resetTask promise 
        // is resolved. 
        this.resetsInProgress.add(indexToReset.name);

        setTimeout(() => {
            this.resetsInProgress.delete(indexToReset.name);
        }, 30000);

        app.showDialog(resetIndexVm);
    }

    deleteIndex(i: index) {
        this.promptDeleteIndexes([i]);
        this.resetsInProgress.delete(i.name);
    }

    processIndexEvent(e: Raven.Abstractions.Data.IndexChangeNotification) {
        const indexRemovedEvent = "IndexRemoved" as Raven.Abstractions.Data.IndexChangeTypes;
        if (e.Type === indexRemovedEvent) {
            if (!this.resetsInProgress.has(e.Name)) {
                this.removeIndexesFromAllGroups(this.findIndexesByName(e.Name));
            }
        } else {
            setTimeout(() => {
                this.fetchIndexes();
            }, 5000);
        }
    }

    private removeIndexesFromAllGroups(indexes: index[]) {
        this.indexGroups().forEach(g => {
            g.indexes.removeAll(indexes);
        });

        // Remove any empty groups.
        this.indexGroups.remove((item: indexGroup) => item.indexes().length === 0);
    }

    private findIndexesByName(indexName: string): index[] {
        var result = new Array<index>();
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
            const deleteIndexesVm = new deleteIndexesConfirm(indexes.map(i => i.name), this.activeDatabase());
            app.showDialog(deleteIndexesVm);
            deleteIndexesVm.deleteTask
                .done((deleted: boolean) => {
                    if (deleted) {
                        this.removeIndexesFromAllGroups(indexes);
                    }
                });
        }
    }

    unlockIndex(i: index) {
        this.updateIndexLockMode(i, "Unlock");
    }

    lockIndex(i: index) {
        this.updateIndexLockMode(i, "LockedIgnore");
    }

    lockErrorIndex(i: index) {
        this.updateIndexLockMode(i, "LockedError");
    }

    lockSideBySide(i: index) {
        this.updateIndexLockMode(i, "SideBySide");
    }

    private updateIndexLockMode(i: index, newLockMode: Raven.Abstractions.Indexing.IndexLockMode) {
        if (i.lockMode() !== newLockMode) {
            this.spinners.localLockChanges.push(i.name);

            new saveIndexLockModeCommand([i], newLockMode, this.activeDatabase())
                .execute()
                .done(() => i.lockMode(newLockMode))
                .always(() => this.spinners.localLockChanges.remove(i.name));
        }
    }

    idlePriority(idx: index) {
        const idle = "Idle" as Raven.Client.Data.Indexes.IndexingPriority;
        const forced = "Forced" as Raven.Client.Data.Indexes.IndexingPriority;
        this.setIndexPriority(idx, idle + "," + forced as any);
    }

    disabledPriority(idx: index) {
        const disabled = "Disabled" as Raven.Client.Data.Indexes.IndexingPriority;
        const forced = "Forced" as Raven.Client.Data.Indexes.IndexingPriority;
        this.setIndexPriority(idx, disabled + "," + forced as any);
    }

    normalPriority(idx: index) {
        this.setIndexPriority(idx, "Normal");
    }

    private setIndexPriority(idx: index, newPriority: Raven.Client.Data.Indexes.IndexingPriority) {
        const originalPriority = idx.priority();
        if (originalPriority !== newPriority) {
            this.spinners.localPriority.push(idx.name);

            new saveIndexPriorityCommand(idx.name, newPriority, this.activeDatabase())
                .execute()
                .done(() => idx.priority(newPriority))
                .always(() => this.spinners.localPriority.remove(idx.name));
        }
    }

    createNotifications(): Array<changeSubscription> {
        return [
            //TODO: it isn't implemented on server side yet: changesContext.currentResourceChangesApi().watchAllIndexes(e => this.processIndexEvent(e)),
            //TODO: use cool down
            this.changesContext.currentResourceChangesApi().watchDocsStartingWith(indexReplaceDocument.replaceDocumentPrefix, () => this.processReplaceEvent())
        ];
    }

    processReplaceEvent() {
        setTimeout(() => this.fetchIndexes(), 10);
    }

    cancelSideBySideIndex(i: index) {
        const cancelSideBySideIndexViewModel = new cancelSideBySizeConfirm([i.name], this.activeDatabase());
        app.showDialog(cancelSideBySideIndexViewModel);
        cancelSideBySideIndexViewModel.cancelTask
            .done((closedWithoutDeletion: boolean) => {
                if (!closedWithoutDeletion) {
                    this.removeIndexesFromAllGroups([i]);
                }
            })
            .fail(() => {
                this.removeIndexesFromAllGroups([i]);
                this.fetchIndexes();
            });
    }

    forceSideBySide(idx: index) {
        new forceIndexReplace(idx.name, this.activeDatabase()).execute();
    }

    setLockModeSelectedIndexes(lockModeString: Raven.Abstractions.Indexing.IndexLockMode, lockModeStrForTitle: string) {
        if (this.lockModeCommon() === lockModeString)
            return;

        this.confirmationMessage("Are you sure?", `Do you want to ${lockModeStrForTitle} selected indexes?`)
            .done(can => {
                if (can) {
                    this.spinners.globalLockChanges(true);

                    const indexes = this.getSelectedIndexes();

                    new saveIndexLockModeCommand(indexes, lockModeString, this.activeDatabase())
                        .execute()
                        .done(() => indexes.forEach(i => i.lockMode(lockModeString)))
                        .always(() => this.spinners.globalLockChanges(false));
                }
            });
    }

    deleteSelectedIndexes() {
        this.promptDeleteIndexes(this.getSelectedIndexes());
    }

    startIndexing(): void {
        this.spinners.globalStartStop(true);
        new toggleIndexingCommand(true, this.activeDatabase())
            .execute()
            .done(() => this.indexingEnabled(true))
            .always(() => {
                this.spinners.globalStartStop(false);
                this.fetchIndexes();
            });
    }

    stopIndexing() {
        this.spinners.globalStartStop(true);
        new toggleIndexingCommand(false, this.activeDatabase())
            .execute()
            .done(() => this.indexingEnabled(false))
            .always(() => {
                this.spinners.globalStartStop(false);
                this.fetchIndexes();
            });
    }

    resumeIndexing(idx: index) {
        this.spinners.localPriority.push(idx.name);

        new toggleIndexingCommand(true, this.activeDatabase(), { name: [idx.name] })
            .execute()
            .done(() => idx.pausedUntilRestart(false))
            .always(() => this.spinners.localPriority.remove(idx.name));
    }

    pauseUntilRestart(idx: index) {
        this.spinners.localPriority.push(idx.name);

        new toggleIndexingCommand(false, this.activeDatabase(), { name: [idx.name] })
            .execute()
            .done(() => idx.pausedUntilRestart(true))
            .always(() => this.spinners.localPriority.remove(idx.name));
    }

    toggleSelectAll() {
        const selectedIndexesCount = this.selectedIndexesName().length;

        if (selectedIndexesCount > 0) {
            this.selectedIndexesName([]);
        } else {
            const namesToSelect = [] as Array<string>;

            this.indexGroups().forEach(indexGroup => {
                if (!indexGroup.groupHidden()) {
                    indexGroup.indexes().forEach(index => {
                        if (!index.filteredOut() && !namesToSelect.contains(index.name)) {
                            namesToSelect.push(index.name);
                        }
                    });
                }
            });
            this.selectedIndexesName(namesToSelect);
        }
    }
}

export = indexes;
