import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import getIndexMergeSuggestionsCommand = require("commands/database/index/getIndexMergeSuggestionsCommand");
import getIndexesStatsCommand = require("commands/database/index/getIndexesStatsCommand");
import moment = require("moment");
import generalUtils = require("common/generalUtils");
import deleteIndexesConfirm from "viewmodels/database/indexes/deleteIndexesConfirm";
import app = require("durandal/app");
import getIndexesDefinitionsCommand = require("commands/database/index/getIndexesDefinitionsCommand");
import indexDefinition from "models/database/index/indexDefinition";
import eventsCollector = require("common/eventsCollector");
import router = require("plugins/router");
import mergedIndexesStorage = require("common/storage/mergedIndexesStorage");
import copyToClipboard = require("common/copyToClipboard");

interface notQueriedIndexInfo {
    name: string;
    lastQueryTime?: string;
    lastIndexingTime?: string;
}

interface indexStats {
    lastQueryTime?: string;
    lastIndexingTime?: string;
}

interface surpassingIndexInfo {
    surpassingIndex: string;
    toDelete: string;
    lastQueryTime?: string;
    lastIndexingTime?: string;
}

interface mergeCandidateIndexInfo {
    toMerge: mergeCandidateIndexItemInfo[];
    mergedIndexDefinition: Raven.Client.Documents.Indexes.IndexDefinition;
}

interface mergeCandidateIndexItemInfo {
    name: string;
    lastQueryTime?: string;
    lastIndexingTime?: string;
}

class indexCleanup extends viewModelBase {

    view = require("views/database/indexes/indexCleanup.html");
    
    appUrls: computedAppUrls;
    
    indexStats: Raven.Client.Documents.Indexes.IndexStats[] = [];
    
    mergeSuggestions = ko.observableArray<mergeCandidateIndexInfo>();
    surpassingSuggestions = ko.observableArray<surpassingIndexInfo>();
    unmergables = ko.observableArray<{ indexName: string; reason: string; }>();
    notQueriedForLastWeek = ko.observableArray<notQueriedIndexInfo>([]);

    mergeSuggestionsErrors = ko.observableArray<mergeSuggestionsError>([]);

    notQueriedSelectionState: KnockoutComputed<checkbox>;
    surpassingSelectionState: KnockoutComputed<checkbox>;
    selectedNotQueriedIndexes = ko.observableArray<string>([]);
    selectedSurpassingIndexes = ko.observableArray<string>([]);
    
    spinners = {
        notQueried: ko.observable<boolean>(false),
        surpassing: ko.observable<boolean>(false)
    }
    
    constructor() {
        super();
        
        this.appUrls = appUrl.forCurrentDatabase();

        this.notQueriedSelectionState = ko.pureComputed<checkbox>(() => {
            const selectedCount = this.selectedNotQueriedIndexes().length;
            const indexesCount = this.notQueriedForLastWeek().length;
            if (indexesCount && selectedCount === indexesCount)
                return "checked";
            if (selectedCount > 0)
                return "some_checked";
            return "unchecked";
        });

        this.surpassingSelectionState = ko.pureComputed<checkbox>(() => {
            const selectedCount = this.selectedSurpassingIndexes().length;
            const indexesCount = this.surpassingSuggestions().length;
            if (indexesCount && selectedCount === indexesCount)
                return "checked";
            if (selectedCount > 0)
                return "some_checked";
            return "unchecked";
        });
        
        this.bindToCurrentInstance("navigateToMergeSuggestion");
    }
  
    canActivate(args: any) :any {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                const deferred = $.Deferred();
                this.reload()
                    .done(() => deferred.resolve({ can: true }))
                    .fail(() => deferred.resolve({ redirect: appUrl.forIndexes(this.activeDatabase()) }));

                return deferred;
            });
    }

    private reload() {
        return this.fetchStats()
            .then(() => {
                return this.fetchIndexMergeSuggestions();
            }).done(() => {
                this.selectedNotQueriedIndexes([]);
                this.selectedSurpassingIndexes([]);
            });
    }

    private fetchStats(): JQueryPromise<Raven.Client.Documents.Indexes.IndexStats[]> {
        const db = this.activeDatabase();
        if (db) {
            return new getIndexesStatsCommand(db)
                .execute()
                .done(result => {
                    this.indexStats = result;
                    this.notQueriedForLastWeek(indexCleanup.findUnusedIndexes(result));
                });
        }

        return null;
    }

    private findIndexStats(name: string): indexStats {
        const matchedIndex = this.indexStats.find(x => x.Name === name);
        return {
            lastIndexingTime: matchedIndex?.LastIndexingTime,
            lastQueryTime: matchedIndex?.LastQueryingTime
        };
    }
    
    private static findUnusedIndexes(stats: Raven.Client.Documents.Indexes.IndexStats[]): notQueriedIndexInfo[] {
        const result: notQueriedIndexInfo[] = [];

        const now = moment();
        const milliSecondsInWeek = 1000 * 3600 * 24 * 7;
        
        for (const stat of stats) {
            if (stat.LastQueryingTime) {
                const lastQueryDate = moment(stat.LastQueryingTime);
                const agoInMs = now.diff(lastQueryDate);
                
                if (lastQueryDate.isValid() && agoInMs > milliSecondsInWeek) {
                    result.push({
                        name: stat.Name,
                        lastQueryTime: stat.LastQueryingTime ? generalUtils.toHumanizedDate(stat.LastQueryingTime) : null,
                        lastIndexingTime: stat.LastIndexingTime ? generalUtils.toHumanizedDate(stat.LastIndexingTime) : null
                    });
                    
                    result.sort((a, b) => a.lastQueryTime.localeCompare(b.lastQueryTime));
                }
            }
        }
        
        return result;
    }

    private fetchIndexMergeSuggestions() {
        const db = this.activeDatabase();
        return new getIndexMergeSuggestionsCommand(db)
            .execute()
            .done((results: Raven.Server.Documents.Indexes.IndexMerging.IndexMergeResults) => {
                const suggestions = results.Suggestions;
                
                const mergeCandidatesRaw = suggestions.filter(x => x.MergedIndex);
                
                this.mergeSuggestions(mergeCandidatesRaw.map(x => {
                    return {
                        mergedIndexDefinition: x.MergedIndex,
                        toMerge: x.CanMerge.map(m => {
                            const stats = this.findIndexStats(m);
                            return {
                                name: m,
                                lastQueryTime: stats.lastQueryTime ? generalUtils.toHumanizedDate(stats.lastQueryTime) : null,
                                lastIndexingTime: stats.lastIndexingTime ? generalUtils.toHumanizedDate(stats.lastIndexingTime) : null
                            }
                        })
                    }
                }));
                
                
                const surpassingRaw = suggestions.filter(x => !x.MergedIndex);
                
                const surpassing: surpassingIndexInfo[] = [];
                surpassingRaw.forEach(group => {
                    group.CanDelete.forEach(deleteCandidate => {
                        const stats = this.findIndexStats(deleteCandidate);
                        
                        surpassing.push({
                            toDelete: deleteCandidate,
                            surpassingIndex: group.SurpassingIndex,
                            lastQueryTime: stats.lastQueryTime ? generalUtils.toHumanizedDate(stats.lastQueryTime) : null,
                            lastIndexingTime: stats.lastIndexingTime ? generalUtils.toHumanizedDate(stats.lastIndexingTime) : null
                        });
                    });
                });
                
                this.surpassingSuggestions(surpassing);
                
                this.unmergables(Object.keys(results.Unmergables).map(key => ({
                    indexName: key,
                    reason: results.Unmergables[key]
                })));

                this.mergeSuggestionsErrors(results.Errors.map(x => new mergeSuggestionsError(x)));
            });
    }

    indexUrl(name: string) {
        return this.appUrls.editIndex(name);
    }
    
    toggleSelectAllNotQueried() {
        const selectedIndexesCount = this.selectedNotQueriedIndexes().length;
        if (selectedIndexesCount > 0) {
            this.selectedNotQueriedIndexes([]);
        } else {
            this.selectedNotQueriedIndexes(this.notQueriedForLastWeek().map(x => x.name));
        }
    }

    toggleSelectAllSurpassing() {
        const selectedIndexesCount = this.selectedSurpassingIndexes().length;
        if (selectedIndexesCount > 0) {
            this.selectedSurpassingIndexes([]);
        } else {
            this.selectedSurpassingIndexes(this.surpassingSuggestions().map(x => x.toDelete));
        }
    }

    deleteSelectedSurpassingIndexes() {
        eventsCollector.default.reportEvent("index-merge-suggestions", "delete-surpassing");
        this.spinners.surpassing(true);
        this.deleteIndexes(this.selectedSurpassingIndexes())
            .always(() => {
                this.spinners.surpassing(false);
            });
    }

    deleteSelectedNotQueriedIndexes() {
        eventsCollector.default.reportEvent("index-merge-suggestions", "delete-unused");
        this.spinners.notQueried(true);
        this.deleteIndexes(this.selectedNotQueriedIndexes())
            .always(() => {
                this.spinners.notQueried(false);
            });
    }
    
    private deleteIndexes(names: string[]) {
        const db = this.activeDatabase();
        return new getIndexesDefinitionsCommand(db, 0, 1024 * 1024)
            .execute()
            .done((indexDefinitions) => {
                const matchedIndexes = indexDefinitions.filter(x => names.includes(x.Name)).map(x => new indexDefinition(x));
                
                const deleteViewModel = new deleteIndexesConfirm(matchedIndexes, this.activeDatabase());
                deleteViewModel.deleteTask.done((done) => {
                    if (done) {
                        this.reload();
                    }
                });
                app.showBootstrapDialog(deleteViewModel);
            });
    }

    navigateToMergeSuggestion(item: mergeCandidateIndexInfo) {
        const db = this.activeDatabase();
        const mergedIndexName = mergedIndexesStorage.saveMergedIndex(db, item.mergedIndexDefinition, item.toMerge.map(x => x.name));
        
        const targetUrl = this.appUrls.editIndex(mergedIndexName)();
        
        router.navigate(targetUrl);
    }
}

class mergeSuggestionsError {
    readonly indexName: string;
    readonly message: string;
    readonly stackTrace: string;

    readonly isStackTraceVisible = ko.observable<boolean>(false);

    constructor(dto: Raven.Server.Documents.Indexes.IndexMerging.MergeError) {
        this.indexName = dto.IndexName;
        this.message = dto.Message;
        this.stackTrace = dto.StackTrace;
    }

    toggleIsStackTraceVisible() {
        this.isStackTraceVisible(!this.isStackTraceVisible());
    }

    copyErrorToClipboard() {
        copyToClipboard.copy(this.message + this.stackTrace, "Error has been copied to clipboard");
    }
}

export = indexCleanup; 
