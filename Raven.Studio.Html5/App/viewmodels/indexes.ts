import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import index = require("models/index");
import appUrl = require("common/appUrl");
import saveIndexLockModeCommand = require("commands/saveIndexLockModeCommand");
import saveIndexAsPersistentCommand = require("commands/saveIndexAsPersistentCommand");
import deleteIndexesConfirm = require("viewmodels/deleteIndexesConfirm");
import querySort = require("models/querySort");
import app = require("durandal/app");
import resetIndexConfirm = require("viewmodels/resetIndexConfirm");
import router = require("plugins/router"); 
import shell = require("viewmodels/shell");
import changeSubscription = require("models/changeSubscription");
import indexesShell = require("viewmodels/indexesShell");
import recentQueriesStorage = require("common/recentQueriesStorage");

class indexes extends viewModelBase {

    indexGroups = ko.observableArray<{ entityName: string; indexes: KnockoutObservableArray<index> }>();
    queryUrl = ko.observable<string>();
    newIndexUrl = appUrl.forCurrentDatabase().newIndex;
    containerSelector = "#indexesContainer";
    recentQueries = ko.observableArray<storedQueryDto>();
    indexMutex = true;
    appUrls: computedAppUrls;
    btnState = ko.observable<boolean>(false);
    btnStateTooltip = ko.observable<string>("ExpandAll");
    btnTitle = ko.computed(() => this.btnState() === true ? "ExpandAll" : "CollapseAll");

    sortedGroups: KnockoutComputed<{ entityName: string; indexes: KnockoutObservableArray<index>; }[]>;

    constructor() {
        super();

        this.sortedGroups = ko.computed(() => {
            var groups = this.indexGroups().slice(0).sort((l, r) => l.entityName.toLowerCase() > r.entityName.toLowerCase() ? 1 : -1);

            groups.forEach((group: { entityName: string; indexes: KnockoutObservableArray<index> }) => {
                group.indexes(group.indexes().slice(0).sort((l: index, r: index) => l.name.toLowerCase() > r.name.toLowerCase() ? 1 : -1));
            });

            return groups;
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

        this.appUrls = appUrl.forCurrentDatabase();
        this.queryUrl(appUrl.forQuery(this.activeDatabase(), null));
    }

    attached() {
        // Alt+Minus and Alt+Plus are already setup. Since laptops don't have a dedicated key for plus, we'll also use the equal sign key (co-opted for plus).
        //this.createKeyboardShortcut("Alt+=", () => this.toggleExpandAll(), this.containerSelector);
        ko.postbox.publish("SetRawJSONUrl", appUrl.forIndexesRawData(this.activeDatabase()));

        var self = this;
        $(window).bind('storage', () => {
            self.fetchRecentQueries();
        });
    }

    private fetchIndexes() {
        return new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((stats: databaseStatisticsDto) => this.processDbStats(stats));
    }

    private fetchRecentQueries() {
        this.recentQueries(recentQueriesStorage.getRecentQueries(this.activeDatabase()));
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

    processDbStats(stats: databaseStatisticsDto) {
        stats.Indexes
            .map(i => new index(i))
            .forEach(i => this.putIndexIntoGroups(i));
    }

    putIndexIntoGroups(i: index) {
        if (!i.forEntityName || i.forEntityName.length === 0) {
            this.putIndexIntoGroupNamed(i, "Other");
        } else {
            i.forEntityName.forEach(e => this.putIndexIntoGroupNamed(i, e));
        }
    }

    putIndexIntoGroupNamed(i: index, groupName: string) {
        var group = this.indexGroups.first(g => g.entityName === groupName);
        var oldIndex: index;
        var indexExists: boolean;
        if (group) {
            oldIndex = group.indexes.first((cur: index) => cur.name == i.name);
            if (!!oldIndex) {
                group.indexes.replace(oldIndex, i);
            } else {
                group.indexes.push(i);
            }
        } else {
            this.indexGroups.push({ entityName: groupName, indexes: ko.observableArray([i]) });
        }
    }

    createNotifications(): Array<changeSubscription> {
        return [shell.currentResourceChangesApi().watchAllIndexes(e => this.processIndexEvent(e))];
    }

    processIndexEvent(e: indexChangeNotificationDto) {
        if (e.Type == indexChangeType.IndexRemoved) {
            this.removeIndexesFromAllGroups(this.findIndexesByName(e.Name));
        } else {
            if (this.indexMutex == true) {
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
                if (i.name == indexName) {
                    result.push(i);
                }
            });
        });

        return result;
    }

    copyIndex(i: index) {
        require(["viewmodels/copyIndexDialog"], copyIndexDialog => {
            app.showDialog(new copyIndexDialog(i.name, this.activeDatabase(), false));
        });
    }

    pasteIndex() {
        require(["viewmodels/copyIndexDialog"], copyIndexDialog => {
            app.showDialog(new copyIndexDialog('', this.activeDatabase(), true));
        });
    }

    showMergeSuggestions() {
        require(["viewmodels/indexMergeSuggestionsDialog"], indexMergeSuggestionsDialog => {
            app.showDialog(new indexMergeSuggestionsDialog(this.activeDatabase()));
        });
    }
    
    toggleExpandAll() {
       if (this.btnState() === true) {
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
        this.promptDeleteIndexes(this.getAllIndexes());
    }

    deleteIndex(i: index) {
        this.promptDeleteIndexes([i]);
    }

    deleteIndexGroup(i: { entityName: string; indexes: KnockoutObservableArray<index> }) {
        this.promptDeleteIndexes(i.indexes());
    }

    promptDeleteIndexes(indexes: index[]) {
        if (indexes.length > 0) {
            var deleteIndexesVm = new deleteIndexesConfirm(indexes.map(i => i.name), this.activeDatabase());
            app.showDialog(deleteIndexesVm);
            deleteIndexesVm.deleteTask
                .done((closedWithoutDeletion: boolean) => {
                    if (closedWithoutDeletion == false) {
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
}

export = indexes;