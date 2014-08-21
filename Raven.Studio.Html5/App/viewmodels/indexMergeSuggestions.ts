import viewModelBase = require("viewmodels/viewModelBase");
import getIndexMergeSuggestionsCommand = require("commands/getIndexMergeSuggestionsCommand");
import database = require("models/database");
import appUrl = require("common/appUrl");
import mergedIndexesStorage = require("common/mergedIndexesStorage");
import indexMergeSuggestion = require("models/indexMergeSuggestion");
import changeSubscription = require('models/changeSubscription');
import shell = require("viewmodels/shell");

class indexMergeSuggestions extends viewModelBase {
    
    appUrls: computedAppUrls;
    suggestions = ko.observableArray<indexMergeSuggestion>();
    unmergables = ko.observableArray<{ indexName: string; reason: string; }>();
    
    constructor() {
        super();
        this.appUrls = appUrl.forCurrentDatabase();
    }

    canActivate(args: any) :any {
        var deferred = $.Deferred();
        
        var fetchIndexMergeSuggestionsTask = this.fetchIndexMergeSuggestions();
        fetchIndexMergeSuggestionsTask
            .done(() => deferred.resolve({ can: true }))
            .fail(() => deferred.resolve({ redirect: appUrl.forIndexes(this.activeDatabase()) }));

        return deferred;
    }

    createNotifications(): Array<changeSubscription> {
        return [ shell.currentResourceChangesApi().watchAllIndexes(() => this.fetchIndexMergeSuggestions()) ];
    }

    activate(args) {
        super.activate(args);

    }

    private fetchIndexMergeSuggestions() {
        var deferred = $.Deferred();

        var db = this.activeDatabase();
        new getIndexMergeSuggestionsCommand(db)
            .execute()
            .done((results: indexMergeSuggestionsDto) => {
                var suggestions = results.Suggestions.map((suggestion: suggestionDto) => new indexMergeSuggestion(suggestion));
                this.suggestions(suggestions);

                var unmergables = Object.keys(results.Unmergables).map((value, index) => {
                    return { indexName: value, reason: results.Unmergables[value] }
                });
                this.unmergables(unmergables);
                deferred.resolve();
            })
            .fail(() => deferred.reject());

        return deferred;
    }

    mergeSuggestionIndex(index: string): number {
        return parseInt(index) + 1;
    }

    mergedIndexUrl(id: string) {
        var db: database = this.activeDatabase();
        var mergedIndexName = mergedIndexesStorage.getMergedIndexName(db, id);

        return this.appUrls.editIndex(mergedIndexName);
    }

    saveMergedIndex(id: string, suggestion: indexMergeSuggestion) {
        var db: database = this.activeDatabase();
        mergedIndexesStorage.saveMergedIndex(db, id, suggestion);

        return true;
    }
}

export = indexMergeSuggestions; 