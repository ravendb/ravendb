import viewModelBase = require("viewmodels/viewModelBase");
import getIndexMergeSuggestionsCommand = require("commands/getIndexMergeSuggestionsCommand");
import indexDefinition = require("models/indexDefinition");
import database = require("models/database");
import appUrl = require("common/appUrl");
import mergedIndexesStorage = require("common/mergedIndexesStorage");


import saveIndexDefinitionCommand = require("commands/saveIndexDefinitionCommand");
import indexPriority = require("models/indexPriority");
import messagePublisher = require("common/messagePublisher");

class indexMergeSuggestions extends viewModelBase {
    
    appUrls: computedAppUrls;
    suggestions = ko.observableArray<{ canMerge: string[]; collection: string; mergedIndex: indexDefinition; }>();
    unmergables = ko.observableArray<{ indexName: string; reason: string; }>();
    
    constructor() {
        super();
        this.appUrls = appUrl.forCurrentDatabase();
    }

    canActivate(args: any) :any {
        var deferred = $.Deferred();
        
        var db = this.activeDatabase();
        var fetchIndexMergeSuggestionsTask = this.fetchIndexMergeSuggestions(db);
        fetchIndexMergeSuggestionsTask
            .done(() => deferred.resolve({ can: true }))
            .fail(() => deferred.resolve({ redirect: appUrl.forIndexes(db) }));

        return deferred;
    }

    activate(args) {
        super.activate(args);

    }

    private fetchIndexMergeSuggestions(db: database) {
        var deferred = $.Deferred();

        new getIndexMergeSuggestionsCommand(db)
            .execute()
            .done((results: indexMergeSuggestionsDto) => {
                var suggestions = results.Suggestions.map((suggestion: suggestionDto) => {
                    return { canMerge: suggestion.CanMerge, collection: "123", mergedIndex: new indexDefinition(suggestion.MergedIndex) }
                });
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

    mergedIndexUrl(mergedIndex: indexDefinition, index: number) {
        var db: database = this.activeDatabase();

        var savedMergedIndexName = mergedIndexesStorage.saveMergedIndex(db, this.makeId(), mergedIndex);

        return this.appUrls.editIndex(savedMergedIndexName);
    }

    showMergedIndex(mergedIndex: indexDefinition) {
        var db: database = this.activeDatabase();
        var savedMergedIndexName = mergedIndexesStorage.saveMergedIndex(db, this.makeId(), mergedIndex);

        return true;
    }

    private makeId() {
        var text = "";
        var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        for (var i = 0; i < 5; i++)
            text += chars.charAt(Math.floor(Math.random() * chars.length));

        return text;
    }

    saveMergedIndex(mergedIndex: indexDefinition, indexesToDelete: string[]) {
        var db = this.activeDatabase();

        this.fetchIndexMergeSuggestions(db);
/*        if (this.isPaste === true && !!this.indexJSON()) {
            var indexDto: indexDefinitionDto;

            try {
                indexDto = JSON.parse(this.indexJSON());
                var testIndex = new indexDefinition(indexDto);
            } catch(e) {
                indexDto = null;
                messagePublisher.reportError("Index paste failed, invalid json string", e);
            }

            if (indexDto) {

                new getIndexDefinitionCommand(indexDto.Name, this.db)
                    .execute()
                    .fail((request, status, error) => {
                        if (request.status === 404) {
                            new saveIndexDefinitionCommand(indexDto, indexPriority.normal, this.db)
                                .execute()
                                .done(() => {
                                    router.navigate(appUrl.forEditIndex(indexDto.Name, this.db));
                                    this.close();
                                });
                        } else {
                            messagePublisher.reportError("Cannot paste index, error occured!", error);
                        }
                    })
                    .done(() => messagePublisher.reportError("Cannot paste index, error occured!", "Index with that name already exists!"));
            } 
        } else {
            this.close();    
        }*/
    }
}

export = indexMergeSuggestions; 