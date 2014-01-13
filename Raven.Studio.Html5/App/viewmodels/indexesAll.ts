import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import activeDbViewModelBase = require("viewmodels/activeDbViewModelBase");
import index = require("models/index");
import appUrl = require("common/appUrl");
import saveIndexLockModeCommand = require("commands/saveIndexLockModeCommand");
import saveIndexAsPersistentCommand = require("commands/saveIndexAsPersistentCommand");
import deleteIndexesConfirm = require("viewmodels/deleteIndexesConfirm");
import app = require("durandal/app");

class indexes extends activeDbViewModelBase {

    indexGroups = ko.observableArray<{ entityName: string; indexes: KnockoutObservableArray<index> }>();
    queryUrl: KnockoutComputed<string>;
    newIndexUrl = appUrl.forCurrentDatabase().newIndex;
    
    activate(args) {
        super.activate(args);

        this.fetchIndexes();
        this.activeDatabase.subscribe(() => this.onDatabaseChanged());
        this.queryUrl = appUrl.forCurrentDatabase().query;
    }

    fetchIndexes() {
        new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((stats: databaseStatisticsDto) => this.processDbStats(stats));
    }

    onDatabaseChanged() {
        this.indexGroups([]);
        this.fetchIndexes();
    }

    processDbStats(stats: databaseStatisticsDto) {
        stats.Indexes
            .map(i => new index(i))
            .forEach(i => this.putIndexIntoGroups(i));
    }

    putIndexIntoGroups(i: index) {
        if (i.forEntityName.length === 0) {
            this.putIndexIntoGroupNamed(i, "Other");
        } else {
            i.forEntityName.forEach(e => this.putIndexIntoGroupNamed(i, e));
        }
    }

    putIndexIntoGroupNamed(i: index, groupName: string) {
        var group = this.indexGroups.first(g => g.entityName === groupName);
        if (group) {
            group.indexes.push(i);
        } else {
            this.indexGroups.push({ entityName: groupName, indexes: ko.observableArray([i]) });
        }
    }

    collapseAll() {
        $(".index-group-content").collapse('hide');
    }

    expandAll() {
        $(".index-group-content").collapse('show');
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

    promptDeleteIndexes(indexes: index[]) {
        if (indexes.length > 0) {
            var deleteIndexesVm = new deleteIndexesConfirm(indexes, this.activeDatabase());
            app.showDialog(deleteIndexesVm);
            deleteIndexesVm.deleteTask.done(() => this.removeIndexesFromAllGroups(indexes));
        }
    }

    removeIndexesFromAllGroups(indexes: index[]) {
        this.indexGroups().forEach(g => {
            g.indexes.removeAll(indexes);
        });
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