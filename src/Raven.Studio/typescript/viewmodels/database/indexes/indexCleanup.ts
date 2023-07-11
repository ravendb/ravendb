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
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database from "models/resources/database";

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

class indexCleanup extends shardViewModelBase {

    view = require("views/database/indexes/indexCleanup.html");
    
    appUrls: computedAppUrls;
    
    indexStats: Map<string, indexStats> = new Map<string, indexStats>();
    
    mergeSuggestions = ko.observableArray<mergeCandidateIndexInfo>();
    surpassingSuggestions = ko.observableArray<surpassingIndexInfo>();
    unmergables = ko.observableArray<{ indexName: string; reason: string; }>();
    notQueriedForLastWeek = ko.observableArray<notQueriedIndexInfo>([]);

    notQueriedSelectionState: KnockoutComputed<checkbox>;
    surpassingSelectionState: KnockoutComputed<checkbox>;
    selectedNotQueriedIndexes = ko.observableArray<string>([]);
    selectedSurpassingIndexes = ko.observableArray<string>([]);
    
    spinners = {
        notQueried: ko.observable<boolean>(false),
        surpassing: ko.observable<boolean>(false)
    }

    constructor(db: database, location: databaseLocationSpecifier) {
        super(db, location);
        
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
                    .then(() => deferred.resolve({ can: true }))
                    .catch(() => deferred.resolve({ redirect: appUrl.forIndexes(this.db) }));

                return deferred;
            });
    }

    private reload() {
        return this.fetchStats()
            .then(() => {
                return this.fetchIndexMergeSuggestions();
            }).then(() => {
                this.selectedNotQueriedIndexes([]);
                this.selectedSurpassingIndexes([]);
            });
    }

    // TODO copy 1:1
    private async fetchStats(): Promise<void> {
        const locations = this.db.getLocations();
        const tasks = locations.map(location => new getIndexesStatsCommand(this.db, location).execute());
        
        const allStats = await Promise.all(tasks);
        
        const resultMap = new Map<string, indexStats>();
        for (const nodeStat of allStats) {
            for (const indexStat of nodeStat) {
                const existing = resultMap.get(indexStat.Name);
                const lastIndexingTime = indexCleanup.getNewer(existing?.lastIndexingTime, indexStat.LastIndexingTime);
                const lastQueryTime = indexCleanup.getNewer(existing?.lastQueryTime, indexStat.LastQueryingTime);
                
                resultMap.set(indexStat.Name, {
                    lastIndexingTime,
                    lastQueryTime
                });
            }
        }
        
        this.indexStats = resultMap;
        
        this.notQueriedForLastWeek(indexCleanup.findUnusedIndexes(this.indexStats));
    }
    
    private static getNewer(date1: string, date2: string) {
        if (!date1) {
            return date2;
        }

        if (!date2) {
            return date1;
        }
        
        return date1.localeCompare(date2) ? date1 : date2;
    }
    
    private static findUnusedIndexes(stats: Map<string, indexStats>): notQueriedIndexInfo[] {
        const result: notQueriedIndexInfo[] = [];

        const now = moment();
        const milliSecondsInWeek = 1000 * 3600 * 24 * 7;
        
        for (const [name, stat] of stats.entries()) {
            if (stat.lastQueryTime) {
                const lastQueryDate = moment(stat.lastQueryTime);
                const agoInMs = now.diff(lastQueryDate);
                
                if (lastQueryDate.isValid() && agoInMs > milliSecondsInWeek) {
                    result.push({
                        name,
                        lastQueryTime: stat.lastQueryTime ? generalUtils.toHumanizedDate(stat.lastQueryTime) : null,
                        lastIndexingTime: stat.lastIndexingTime ? generalUtils.toHumanizedDate(stat.lastIndexingTime) : null
                    });
                    
                    result.sort((a, b) => a.lastQueryTime.localeCompare(b.lastQueryTime));
                }
            }
        }
        
        return result;
    }

    private fetchIndexMergeSuggestions() {
        return new getIndexMergeSuggestionsCommand(this.db)
            .execute()
            .done((results: Raven.Server.Documents.Indexes.IndexMerging.IndexMergeResults) => {
                const suggestions = results.Suggestions;
                
                const mergeCandidatesRaw = suggestions.filter(x => x.MergedIndex);
                
                this.mergeSuggestions(mergeCandidatesRaw.map(x => {
                    return {
                        mergedIndexDefinition: x.MergedIndex,
                        toMerge: x.CanMerge.map(m => {
                            const stats = this.indexStats.get(m);
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
                        const stats = this.indexStats.get(deleteCandidate);
                        
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
        return new getIndexesDefinitionsCommand(this.db, 0, 1024 * 1024)
            .execute()
            .done((indexDefinitions) => {
                const matchedIndexes = indexDefinitions.filter(x => names.includes(x.Name)).map(x => new indexDefinition(x));
                
                const deleteViewModel = new deleteIndexesConfirm(matchedIndexes, this.db);
                deleteViewModel.deleteTask.done((done) => {
                    if (done) {
                        this.reload();
                    }
                });
                app.showBootstrapDialog(deleteViewModel);
            });
    }

    navigateToMergeSuggestion(item: mergeCandidateIndexInfo) {
        const mergedIndexName = mergedIndexesStorage.saveMergedIndex(this.db, item.mergedIndexDefinition, item.toMerge.map(x => x.name));
        
        const targetUrl = this.appUrls.editIndex(mergedIndexName)();
        
        router.navigate(targetUrl);
    }
}

export = indexCleanup; 
