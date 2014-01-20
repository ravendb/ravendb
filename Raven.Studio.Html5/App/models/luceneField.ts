class luceneField {
    name = ko.observable<string>();
    stores = ko.observable<string>();
    indexing = ko.observable<string>();
    sort = ko.observable<string>();
    analyzer = ko.observable<string>();
    suggestionDistance = ko.observable<string>();
    suggestionAccuracy = ko.observable<number>();
    termVector = ko.observable<string>();

    constructor(name: string, stores: string = "No", indexing: string = "Default", sort: string = "None", analyzer: string = null, suggestionDistance: string = "None", suggestionAccuracy: number = 0.5, termVector: string = "No") {
        this.name(name);
        this.stores(stores);
        this.indexing(indexing);
        this.sort(sort);
        this.analyzer(analyzer);
        this.suggestionAccuracy(suggestionAccuracy);
        this.suggestionDistance(suggestionDistance);
        this.termVector(termVector);
    }

    toSuggestionDto(): spatialIndexSuggestionDto {
        return {
            Distance: this.suggestionDistance(),
            Accuracy: this.suggestionAccuracy()
        };
    }
}

export = luceneField; 