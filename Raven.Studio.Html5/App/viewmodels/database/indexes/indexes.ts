import getIndexesStatsCommand = require("commands/database/index/getIndexesStatsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import index = require("models/database/index/index");
import appUrl = require("common/appUrl");
import saveIndexLockModeCommand = require("commands/database/index/saveIndexLockModeCommand");
import saveIndexAsPersistentCommand = require("commands/database/index/saveIndexAsPersistentCommand");
import querySort = require("models/database/query/querySort");
import app = require("durandal/app");
import resetIndexConfirm = require("viewmodels/database/indexes/resetIndexConfirm");
import changeSubscription = require("common/changeSubscription");
import recentQueriesStorage = require("common/recentQueriesStorage");
import changesContext = require("common/changesContext");
import copyIndexDialog = require("viewmodels/database/indexes/copyIndexDialog");
import indexesAndTransformersClipboardDialog = require("viewmodels/database/indexes/indexesAndTransformersClipboardDialog");
import indexReplaceDocument = require("models/database/index/indexReplaceDocument");
import getPendingIndexReplacementsCommand = require("commands/database/index/getPendingIndexReplacementsCommand");
import d3 = require('d3/d3');
import cancelSideBySizeConfirm = require("viewmodels/database/indexes/cancelSideBySizeConfirm");
import deleteIndexesConfirm = require("viewmodels/database/indexes/deleteIndexesConfirm");
import forceIndexReplace = require("commands/database/index/forceIndexReplace");
import saveIndexPriorityCommand = require("commands/database/index/saveIndexPriorityCommand");
import indexPriority = require("models/database/index/indexPriority");
import tryRecoverCorruptedIndexes = require("commands/database/index/tryRecoverCorruptedIndexes");
import indexLockAllConfirm = require("viewmodels/database/indexes/indexLockAllConfirm");

class indexes extends viewModelBase {

    resetsInProgress = d3.set();

    indexGroups = ko.observableArray<{ 
        entityName: string; 
        indexes: KnockoutObservableArray<index>; 
        groupHidden: KnockoutObservable<boolean>;
    }>();
    queryUrl = ko.observable<string>();
    newIndexUrl = appUrl.forCurrentDatabase().newIndex;
    containerSelector = "#indexesContainer";
    recentQueries = ko.observableArray<storedQueryDto>();
    indexMutex = true;
    btnState = ko.observable<boolean>(false);
    btnStateTooltip = ko.observable<string>("ExpandAll");
    btnTitle = ko.computed(() => this.btnState() ? "Expand all" : "Collapse all");
    sortedGroups: KnockoutComputed<{ entityName: string; indexes: KnockoutObservableArray<index>; }[]>;
    corruptedIndexes: KnockoutComputed<index[]>;
    lockModeCommon: KnockoutComputed<string>;
    searchText = ko.observable<string>();
    summary: KnockoutComputed<string>;

    constructor() {
        super();
        this.searchText.extend({ throttle: 200 }).subscribe(() => this.filterIndexes());

        this.sortedGroups = ko.computed(() => {
            var groups = this.indexGroups().slice(0).sort((l, r) => l.entityName.toLowerCase() > r.entityName.toLowerCase() ? 1 : -1);

            groups.forEach((group: { entityName: string; indexes: KnockoutObservableArray<index> }) => {
                group.indexes(group.indexes().slice(0).sort((l: index, r: index) => l.name.toLowerCase() > r.name.toLowerCase() ? 1 : -1));
            });

            return groups;
        });

        this.corruptedIndexes = ko.computed(() => {
            var corrupted: index[] = [];
            this.indexGroups().forEach(g => corrupted.pushAll(g.indexes().filter(i => i.priority && i.priority.indexOf(index.priorityErrored) !== -1)));

            return corrupted.distinct();
        });

        this.lockModeCommon = ko.computed(() => {
            var allIndexes = this.getAllIndexes();
            if (allIndexes.length === 0)
                return "None";

            var firstLockMode = allIndexes[0].lockMode();
            for (var i = 1; i < allIndexes.length; i++) {
                if (allIndexes[i].lockMode() !== firstLockMode) {
                    return "Mixed";
                }
            }
            return firstLockMode;
        });

        
        this.summary = ko.computed(() => {
            var indexesCount = 0;
            var mapReduceCount = 0;
            this.indexGroups().forEach(g => {
                g.indexes().forEach(index => {
                    indexesCount += 1;
                    if (index.isMapReduce()) {
                        mapReduceCount++;
                    }
                });
            });

            var summary = "";
            if (indexesCount === 0) {
                return summary;
            }

            summary += indexesCount + " index";
            if (indexesCount > 1) {
                summary += "es";
            }

            var groupsCount = this.indexGroups().length;
            summary += " for " + groupsCount + " collection";
            if (groupsCount > 1) {
                summary += "s";
            }

            if (mapReduceCount > 0) {
                summary += " (" + mapReduceCount + " MapReduce)";
            }

            return summary;
        });
    }

    canActivate(args) {
        super.canActivate(args);

        var deferred = $.Deferred();

        this.fetchRecentQueries();

        $.when(this.fetchIndexes())
            .done(() => deferred.resolve({ can: true }))
            .fail(() => deferred.resolve({ can: false }));

        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('AIHAR1');

        this.queryUrl(appUrl.forQuery(this.activeDatabase(), null));
    }

    attached() {
        super.attached();
        // Alt+Minus and Alt+Plus are already setup. Since laptops don't have a dedicated key for plus, we'll also use the equal sign key (co-opted for plus).
        //this.createKeyboardShortcut("Alt+=", () => this.toggleExpandAll(), this.containerSelector);
        ko.postbox.publish("SetRawJSONUrl", appUrl.forIndexesRawData(this.activeDatabase()));

        var self = this;
        $(window).bind('storage', () => {
            self.fetchRecentQueries();
        });
    }

    idlePriority(idx: index) {
        this.setIndexPriority(idx, indexPriority.idleForced);
    }

    disabledPriority(idx: index) {
        this.setIndexPriority(idx, indexPriority.disabledForced);
    }

    abandonedPriority(idx: index) {
        this.setIndexPriority(idx, indexPriority.abandonedForced);
    }

    normalPriority(idx: index) {
        this.setIndexPriority(idx, indexPriority.normal);
    }

    private setIndexPriority(idx: index, newPriority: indexPriority) {
        new saveIndexPriorityCommand(idx.name, newPriority, this.activeDatabase())
            .execute()
            .done(() => {
                this.fetchIndexes();
            });
    }

    private fetchIndexes() {
        var deferred = $.Deferred();
        var db = this.activeDatabase();

        var statsTask = new getIndexesStatsCommand(db).execute();
        var replacementTask = new getPendingIndexReplacementsCommand(db).execute();

        $.when(statsTask, replacementTask)
            .done((statsTaskResult, replacements: indexReplaceDocument[]) => {

                var stats = statsTaskResult[0];
                this.processData(stats, replacements);

                deferred.resolve(stats);
            })
            .fail(xhr => deferred.reject(xhr));

        return deferred.promise();
    }

    private fetchRecentQueries() {
        this.recentQueries(recentQueriesStorage.getRecentQueries(this.activeDatabase()));
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

    getRecentQueryUrl(query: storedQueryDto) {
        return appUrl.forQuery(this.activeDatabase(), query.Hash);
    }

    getRecentQuerySortText(sorts: string[]) {
        if (sorts.length > 0) {
            return sorts
                .map(s => querySort.fromQuerySortString(s))
                .map(s => s.toHumanizedString())
                .reduce((first, second) => first + ", " + second);
        }

        return "";
    }

    getStoredQueryTransformerParameters(queryParams: Array<transformerParamDto>): string {
        if (queryParams.length > 0) {
            return "(" +
                queryParams
                    .map((param: transformerParamDto) => param.name + "=" + param.value)
                    .join(", ") + ")";
        }

        return "";
    }

    processData(stats: indexStatisticsDto[], replacements: indexReplaceDocument[]) {
        var willReplaceMap = d3.map([]);
        var willBeReplacedMap = d3.map([]);
        replacements.forEach(r => {
            willBeReplacedMap.set(r.indexToReplace, r.extractReplaceWithIndexName());
            willReplaceMap.set(r.extractReplaceWithIndexName(), r.indexToReplace);
        });

        stats.map(i => {
                var idx = new index(i);
                if (willBeReplacedMap.has(idx.name)) {
                    idx.willBeReplacedByIndex(willBeReplacedMap.get(idx.name));
                }
                if (willReplaceMap.has(idx.name)) {
                    idx.willReplaceIndex(willReplaceMap.get(idx.name));
                }
                return idx;
            })
            .forEach(i => this.putIndexIntoGroups(i));
    }

    putIndexIntoGroups(i: index) {
        if (!i.forEntityName || i.forEntityName.length === 0) {
            this.putIndexIntoGroupNamed(i, "Other");
        } else {
            this.putIndexIntoGroupNamed(i, this.getGroupName(i));
        }
    }

    getGroupName(i: index) {
        return i.forEntityName.sort((l, r) => l.toLowerCase() > r.toLowerCase() ? 1 : -1).join(", ");
    }

    putIndexIntoGroupNamed(i: index, groupName: string) {
        var group = this.indexGroups.first(g => g.entityName === groupName);
        var oldIndex: index;
        if (group) {
            oldIndex = group.indexes.first((cur: index) => cur.name === i.name);
            if (!!oldIndex) {
                group.indexes.replace(oldIndex, i);
            } else {
                group.indexes.push(i);
            }
        } else {
            this.indexGroups.push({ 
                entityName: groupName, 
                indexes: ko.observableArray([i]), 
                groupHidden: ko.observable<boolean>(false) });
        }
    }

    createNotifications(): Array<changeSubscription> {
        return [
            changesContext.currentResourceChangesApi().watchAllIndexes(e => this.processIndexEvent(e)),
            changesContext.currentResourceChangesApi().watchDocsStartingWith(indexReplaceDocument.replaceDocumentPrefix, () => this.processReplaceEvent())
        ];
    }

    processReplaceEvent() {
         if (this.indexMutex) {
            this.indexMutex = false;
            setTimeout(() => {
                this.fetchIndexes().always(() => this.indexMutex = true);
            }, 10);
        }
    }

    processIndexEvent(e: indexChangeNotificationDto) {
        if (e.Type === "IndexRemoved") {
            if (!this.resetsInProgress.has(e.Name)) {
                this.removeIndexesFromAllGroups(this.findIndexesByName(e.Name));
            }
        } else {
            if (this.indexMutex) {
                this.indexMutex = false;
                setTimeout(() => {
                    this.fetchIndexes().always(() => this.indexMutex = true);
                }, 5000);
            }
        }
    }

    findIndexesByName(indexName: string) {
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

    copyIndex(i: index) {
        app.showDialog(new copyIndexDialog(i.name, this.activeDatabase(), false));
    }

    pasteIndex() {
        app.showDialog(new copyIndexDialog('', this.activeDatabase(), true));
    }

    copyIndexesAndTransformers() {
        app.showDialog(new indexesAndTransformersClipboardDialog(this.activeDatabase(), false));
    }

    pasteIndexesAndTransformers() {
        var dialog = new indexesAndTransformersClipboardDialog(this.activeDatabase(), true);
        app.showDialog(dialog);
        dialog.pasteDeferred.done((summary: string) => {
            this.confirmationMessage("Indexes And Transformers Paste Summary", summary, ['Ok']);
        });
    }
    toggleExpandAll() {
        if (this.btnState()) {
            $(".index-group-content").collapse('show');
        } else {
            $(".index-group-content").collapse('hide');
        }
        
        this.btnState.toggle();
    }

    deleteIdleIndexes() {
        var idleIndexes = this.getAllIndexes().filter(i => i.priority && i.priority.indexOf("Idle") !== -1);
        this.promptDeleteIndexes(idleIndexes);
    }

    deleteDisabledIndexes() {
        var abandonedIndexes = this.getAllIndexes().filter(i => i.priority && i.priority.indexOf("Disabled") !== -1);
        this.promptDeleteIndexes(abandonedIndexes);
    }

    deleteAbandonedIndexes() {
        var abandonedIndexes = this.getAllIndexes().filter(i => i.priority && i.priority.indexOf("Abandoned") !== -1);
        this.promptDeleteIndexes(abandonedIndexes);
    }

    deleteAllIndexes() {
        this.promptDeleteIndexes(this.getAllIndexes().filter(i => i.name !== "Raven/DocumentsByEntityName"));
    }

    deleteIndex(i: index) {
        this.promptDeleteIndexes([i]);
        this.resetsInProgress.remove(i.name);
    }

    deleteIndexGroup(i: { entityName: string; indexes: KnockoutObservableArray<index> }) {
        this.promptDeleteIndexes(i.indexes());
    }

    cancelIndex(i: index) {
        var cancelSideBySideIndexViewModel = new cancelSideBySizeConfirm([i.name], this.activeDatabase());
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

    promptDeleteIndexes(indexes: index[]) {
        if (indexes.length > 0) {
            var deleteIndexesVm = new deleteIndexesConfirm(indexes.map(i => i.name), this.activeDatabase());
            app.showDialog(deleteIndexesVm);
            deleteIndexesVm.deleteTask
                .done((closedWithoutDeletion: boolean) => {
                    if (!closedWithoutDeletion) {
                        this.removeIndexesFromAllGroups(indexes);
                    }
                })
                .fail(() => {
                    this.removeIndexesFromAllGroups(indexes);
                    this.fetchIndexes();
            });
        }
    }


    resetIndex(indexToReset: index) {
        var resetIndexVm = new resetIndexConfirm(indexToReset.name, this.activeDatabase());

        // reset index is implemented as delete and insert, so we receive notification about deleted index via changes API
        // let's issue marker to ignore index delete information for next few seconds because it might be caused by reset.
        // Unfortunettely we can't use resetIndexVm.resetTask.done, because we receive event via changes api before resetTask promise 
        // if resolved. 
        this.resetsInProgress.add(indexToReset.name);

        setTimeout(() => {
            this.resetsInProgress.remove(indexToReset.name);
        }, 30000);
        
        app.showDialog(resetIndexVm);
    }
    
    removeIndexesFromAllGroups(indexes: index[]) {
        this.indexGroups().forEach(g => {
            g.indexes.removeAll(indexes);
        });

        // Remove any empty groups.
        this.indexGroups.remove((item: { entityName: string; indexes: KnockoutObservableArray<index> }) => item.indexes().length === 0);
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
        this.updateIndexLockMode(i, 'SideBySide');
    }

    updateIndexLockMode(i: index, newLockMode: string) {
        // The old Studio would prompt if you were sure.
        // However, changing the lock status is easily reversible, so we're skipping the prompt.

        var originalLockMode = i.lockMode();
        if (originalLockMode !== newLockMode) {
            i.lockMode(newLockMode);

            new saveIndexLockModeCommand(i, newLockMode, this.activeDatabase())
                .execute()
                .fail(() => i.lockMode(originalLockMode));
        }
    }

    getAllIndexes(): index[]{
        var all: index[] = [];
        this.indexGroups().forEach(g => all.pushAll(g.indexes()));
        return all.distinct();
    }

    makeIndexPersistent(index: index) {
        new saveIndexAsPersistentCommand(index, this.activeDatabase()).execute();
    }

    forceSideBySide(idx: index) {
        new forceIndexReplace(idx.name, this.activeDatabase()).execute();
    }

    tryRecoverCorruptedIndexes() {
        new tryRecoverCorruptedIndexes(this.activeDatabase()).execute();
    }

    setLockModeAllIndexes(lockModeString: string, lockModeStrForTitle: string) {
        if (this.lockModeCommon() === lockModeString)
            return;

        var lockModeTitle = "Do you want to " + lockModeStrForTitle + " ALL Indexes?";

        var indexLockAllVm = new indexLockAllConfirm(lockModeString, this.activeDatabase(), this.getAllIndexes(), lockModeTitle);
        app.showDialog(indexLockAllVm);
    }
}

export = indexes;
