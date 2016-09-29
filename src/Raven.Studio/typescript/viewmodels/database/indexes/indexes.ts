import viewModelBase = require("viewmodels/viewModelBase");
import index = require("models/database/index/index");
import appUrl = require("common/appUrl");
import saveIndexLockModeCommand = require("commands/database/index/saveIndexLockModeCommand");
import app = require("durandal/app");
import resetIndexConfirm = require("viewmodels/database/indexes/resetIndexConfirm");
import changeSubscription = require("common/changeSubscription");
import changesContext = require("common/changesContext");
import copyIndexDialog = require("viewmodels/database/indexes/copyIndexDialog");
import indexesAndTransformersClipboardDialog = require("viewmodels/database/indexes/indexesAndTransformersClipboardDialog");
import indexReplaceDocument = require("models/database/index/indexReplaceDocument");
import getPendingIndexReplacementsCommand = require("commands/database/index/getPendingIndexReplacementsCommand");
import cancelSideBySizeConfirm = require("viewmodels/database/indexes/cancelSideBySizeConfirm");
import deleteIndexesConfirm = require("viewmodels/database/indexes/deleteIndexesConfirm");
import forceIndexReplace = require("commands/database/index/forceIndexReplace");
import saveIndexPriorityCommand = require("commands/database/index/saveIndexPriorityCommand");
import indexLockSelectedConfirm = require("viewmodels/database/indexes/indexLockSelectedConfirm");
import getIndexStatsCommand = require("commands/database/index/getIndexStatsCommand");
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

    indexingEnabled = ko.observable<boolean>(true); //TODO: populate this value from server

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
            var selectedIndexes = this.getSelectedIndexes();
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
        const statsTask = new getIndexStatsCommand(this.activeDatabase())
            .execute();

        const replacementTask = new getPendingIndexReplacementsCommand(this.activeDatabase()).execute(); //TODO: this is not working yet!

        return $.when<any>(statsTask, replacementTask)
            .done(([stats]: [Array<Raven.Client.Data.Indexes.IndexStats>], [replacements]: [indexReplaceDocument[]]) => this.processData(stats, replacements));
    }

    processData(stats: Array<Raven.Client.Data.Indexes.IndexStats>, replacements: indexReplaceDocument[]) {
        //TODO: handle replacements

        stats
            .map(i => new index(i))
            .forEach(i => this.putIndexIntoGroups(i));
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
        var filterLower = this.searchText().toLowerCase();
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

    copyIndex(i: index) {
        app.showDialog(new copyIndexDialog(i.name, this.activeDatabase(), false));
    }

    copySelectedIndexes() {
        alert("implement me!"); //TODO:
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
        const originalLockMode = i.lockMode();
        if (originalLockMode !== newLockMode) {
            i.lockMode(newLockMode);

            new saveIndexLockModeCommand(i, newLockMode, this.activeDatabase())
                .execute()
                .fail(() => i.lockMode(originalLockMode));
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
            idx.priority(newPriority);

            new saveIndexPriorityCommand(idx.name, newPriority, this.activeDatabase())
                .execute()
                .fail(() => idx.priority(originalPriority));
        }
    }

    createNotifications(): Array<changeSubscription> {
        return [
            //TODO: it isn't implemented on server side yet: changesContext.currentResourceChangesApi().watchAllIndexes(e => this.processIndexEvent(e)),
            changesContext.currentResourceChangesApi().watchDocsStartingWith(indexReplaceDocument.replaceDocumentPrefix, () => this.processReplaceEvent())
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

        const lockModeTitle = `Do you want to ${lockModeStrForTitle} selected indexes?`;

        const indexLockAllVm = new indexLockSelectedConfirm(lockModeString, this.activeDatabase(), this.getSelectedIndexes(), lockModeTitle);
        app.showDialog(indexLockAllVm);
    }

    deleteSelectedIndexes() {
        this.promptDeleteIndexes(this.getSelectedIndexes());
    }

    pasteIndex() { //TODO: do we need this method in this class?
        app.showDialog(new copyIndexDialog('', this.activeDatabase(), true));
    }

    copyIndexesAndTransformers() {//TODO: do we need this method in this class?
        app.showDialog(new indexesAndTransformersClipboardDialog(this.activeDatabase(), false));
    }

    pasteIndexesAndTransformers() {//TODO: do we need this method in this class?
        var dialog = new indexesAndTransformersClipboardDialog(this.activeDatabase(), true);
        app.showDialog(dialog);
        dialog.pasteDeferred.done((summary: string) => {
            this.confirmationMessage("Indexes And Transformers Paste Summary", summary, ['Ok']);
        });
    }

    startIndexing(): void {
        this.indexingEnabled(true);
        new toggleIndexingCommand(true, this.activeDatabase())
            .execute()
            .fail(() => this.indexingEnabled(false));
    }

    stopIndexing() {
        this.indexingEnabled(false);
        new toggleIndexingCommand(false, this.activeDatabase())
            .execute()
            .fail(() => this.indexingEnabled(true));
    }
}

export = indexes;
