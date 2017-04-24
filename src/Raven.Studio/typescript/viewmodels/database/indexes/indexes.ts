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
import togglePauseIndexingCommand = require("commands/database/index/togglePauseIndexingCommand");
import eventsCollector = require("common/eventsCollector");
import enableIndexCommand = require("commands/database/index/enableIndexCommand");
import disableIndexCommand = require("commands/database/index/disableIndexCommand");
import computeIndexingProgressCommand = require("commands/database/index/computeIndexingProgressCommand");
import observableMap = require("common/helpers/observableMap");
import indexProgress = require("models/database/index/indexProgress");

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
    indexingProgresses = new observableMap<string, indexProgress>();

    spinners = {
        globalStartStop: ko.observable<boolean>(false),
        globalLockChanges: ko.observable<boolean>(false),
        localPriority: ko.observableArray<string>([]),
        localLockChanges: ko.observableArray<string>([]),
        localState: ko.observableArray<string>([]),
        swapNow: ko.observableArray<string>([]),
        indexingProgress: ko.observableArray<string>([])
    }

    globalIndexingStatus = ko.observable<Raven.Client.Documents.Indexes.IndexRunningStatus>();

    resetsInProgress = new Set<string>();

    throttledRefresh: Function;

    indexErrorsUrl = ko.pureComputed(() => appUrl.forIndexErrors(this.activeDatabase()));

    constructor() {
        super();
        this.initObservables();
        this.bindToCurrentInstance(
            "lowPriority", "highPriority", "normalPriority",
            "resetIndex", "deleteIndex",
            "forceSideBySide", "showProgress",
            "unlockIndex", "lockIndex", "lockErrorIndex",
            "enableIndex", "disableIndex", "disableSelectedIndexes", "enableSelectedIndexes",
            "pauseSelectedIndexes", "resumeSelectedIndexes",
            "unlockSelectedIndexes", "lockSelectedIndexes", "lockErrorSelectedIndexes",
            "deleteSelectedIndexes", "startIndexing", "stopIndexing", "resumeIndexing", "pauseUntilRestart", "toggleSelectAll"
        );

        this.throttledRefresh = _.throttle(() => setTimeout(() => this.fetchIndexes(), 2000), 5000); // refresh not othen then 5 seconds, but delay refresh by 2 seconds
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
        this.searchText.throttle(200).subscribe(() => this.filterIndexes());

        this.sortedGroups = ko.pureComputed<indexGroup[]>(() => {
            var groups = this.indexGroups().slice(0).sort((l, r) => l.entityName.toLowerCase() > r.entityName.toLowerCase() ? 1 : -1);

            groups.forEach((group: { entityName: string; indexes: KnockoutObservableArray<index> }) => {
                group.indexes(group.indexes().slice(0).sort((l: index, r: index) => l.name.toLowerCase() > r.name.toLowerCase() ? 1 : -1));
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

        return $.when<any>(statsTask, statusTask)
            .done(([stats]: [Array<Raven.Client.Documents.Indexes.IndexStats>], [statuses]: [Raven.Client.Documents.Indexes.IndexingStatus]) => this.processData(stats, statuses));
    }

    private processData(stats: Array<Raven.Client.Documents.Indexes.IndexStats>, statuses: Raven.Client.Documents.Indexes.IndexingStatus) {
        this.globalIndexingStatus(statuses.Status);

        const replacements = stats
            .filter(i => i.Name.startsWith(index.SideBySideIndexPrefix));

        stats
            .filter(i => !i.Name.startsWith(index.SideBySideIndexPrefix))
            .map(i => new index(i, this.globalIndexingStatus ))
            .forEach(i => {
                this.putIndexIntoGroups(i);
            });

        this.processReplacements(replacements);
    }

    private processReplacements(replacements: Raven.Client.Documents.Indexes.IndexStats[]) {
        const replacementCache = new Map<string, Raven.Client.Documents.Indexes.IndexStats>();

        replacements.forEach(item => {
            const forIndex = item.Name.substr(index.SideBySideIndexPrefix.length);
            replacementCache.set(forIndex, item);
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
        this.putIndexIntoGroupNamed(i, i.getGroupName());
    }

    private putIndexIntoGroupNamed(i: index, groupName: string): void {
        const group = this.indexGroups().find(g => g.entityName === groupName);
        if (group) {
            const oldIndex = group.indexes().find((cur: index) => cur.name === i.name);
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

    showProgress(idx: index) {
        if (!idx.isStale()) {
            return;
        }

        this.confirmationMessage("Are you sure?", "Computing indexing progress is resource intensive and may take a while.", ["Cancel", "Show indexing progress"])
            .done(result => {
                if (result.can) {
                    this.spinners.indexingProgress.push(idx.name);
                    this.indexingProgresses.delete(idx.name);

                    new computeIndexingProgressCommand(idx.name, this.activeDatabase())
                        .execute()
                        .done(progress => {
                            this.indexingProgresses.set(idx.name, progress);
                        })
                        .always(() => this.spinners.indexingProgress.remove(idx.name));
                }
            });
    }

    resetIndex(indexToReset: index) {
        this.confirmationMessage("Reset index?", "You're resetting " + indexToReset.name)
            .done(result => {
                if (result.can) {

                    eventsCollector.default.reportEvent("indexes", "reset");

                    // reset index is implemented as delete and insert, so we receive notification about deleted index via changes API
                    // let's issue marker to ignore index delete information for next few seconds because it might be caused by reset.
                    // Unfortunately we can't use resetIndexVm.resetTask.done, because we receive event via changes api before resetTask promise 
                    // is resolved. 
                    this.resetsInProgress.add(indexToReset.name);

                    new resetIndexCommand(indexToReset.name, this.activeDatabase())
                        .execute();

                    setTimeout(() => {
                        this.resetsInProgress.delete(indexToReset.name);
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
        const indexRemovedEvent = "IndexRemoved" as Raven.Client.Documents.Changes.IndexChangeTypes;
        if (e.Type === indexRemovedEvent) {
            if (!this.resetsInProgress.has(e.Name)) {
                this.removeIndexesFromAllGroups(this.findIndexesByName(e.Name));
            }
        } else {
            this.throttledRefresh();
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
        this.confirmationMessage("Are you sure?", `Do you want to forcibly swap side-by-side index: ${idx.name}?`)
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

        this.confirmationMessage("Are you sure?", `Do you want to ${lockModeStrForTitle} selected indexes?`)
            .done(can => {
                if (can) {
                    eventsCollector.default.reportEvent("index", "set-lock-mode-selected", lockModeString);

                    this.spinners.globalLockChanges(true);

                    const indexes = this.getSelectedIndexes();

                    new saveIndexLockModeCommand(indexes, lockModeString, this.activeDatabase(),lockModeStrForTitle)
                        .execute()
                        .done(() => indexes.forEach(i => i.lockMode(lockModeString)))
                        .always(() => this.spinners.globalLockChanges(false));
                }
            });
    }

    disableSelectedIndexes() {
        this.toggleDisableSelectedIndexes(false);
    }

    enableSelectedIndexes() {
        this.toggleDisableSelectedIndexes(true);
    }

    private toggleDisableSelectedIndexes(start: boolean) {
        const status = start ? "enable" : "disable";
        this.confirmationMessage("Are you sure?", `Do you want to ${status} selected indexes?`)
            .done(can => {
                if (can) {
                    eventsCollector.default.reportEvent("index", "toggle-status", status);

                    this.spinners.globalLockChanges(true);

                    const indexes = this.getSelectedIndexes();
                    indexes.forEach(i => start ? this.enableIndex(i) : this.disableIndex(i));
                }
            })
            .always(() => this.spinners.globalLockChanges(false));
    }


    pauseSelectedIndexes() {
        this.togglePauseSelectedIndexes(false);
    }

    resumeSelectedIndexes() {
        this.togglePauseSelectedIndexes(true);
    }

    private togglePauseSelectedIndexes(resume: boolean) {
        const status = resume ? "resume" : "pause";
        this.confirmationMessage("Are you sure?", `Do you want to ${status} selected indexes?`)
            .done(can => {
                if (can) {
                    eventsCollector.default.reportEvent("index", "toggle-status", status);

                    this.spinners.globalLockChanges(true);

                    const indexes = this.getSelectedIndexes();
                    indexes.forEach(i => resume ? this.resumeIndexing(i) : this.pauseUntilRestart(i));
                }
            })
            .always(() => this.spinners.globalLockChanges(false));
    }

    deleteSelectedIndexes() {
        eventsCollector.default.reportEvent("indexes", "delete-selected");
        this.promptDeleteIndexes(this.getSelectedIndexes());
    }

    startIndexing(): void {
        this.confirmationMessage("Are you sure?", "Do you want to resume indexing?")
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
        this.confirmationMessage("Are you sure?", "Do you want to pause indexing until server restart?")
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

    enableIndex(idx: index) {
        eventsCollector.default.reportEvent("indexes", "set-state", "enabled");

        if (idx.canBeEnabled()) {
            this.spinners.localState.push(idx.name);

            new enableIndexCommand(idx.name, this.activeDatabase())
                .execute()
                .done(() => {
                    idx.state("Normal");
                    idx.status("Running");
                })
                .always(() => this.spinners.localState.remove(idx.name));
        }
    }

    disableIndex(idx: index) {
        eventsCollector.default.reportEvent("indexes", "set-state", "disabled");

        if (idx.canBeDisabled()) {
            this.spinners.localState.push(idx.name);

            new disableIndexCommand(idx.name, this.activeDatabase())
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
}

export = indexes;
