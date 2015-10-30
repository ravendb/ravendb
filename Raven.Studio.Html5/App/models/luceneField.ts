class luceneField {
    name = ko.observable<string>().extend({ required: true });
    stores = ko.observable<string>().extend({ required: true });
    sort = ko.observable<string>().extend({ required: true });
    termVector = ko.observable<string>().extend({ required: true });
    indexing = ko.observable<string>().extend({ required: true });
    analyzer = ko.observable<string>().extend({ required: true });
    suggestionDistance = ko.observable<string>().extend({ required: true });
    suggestionAccuracy = ko.observable<number>().extend({ required: true });
    
    fieldNameAutocompletes = ko.observableArray<string>();

    constructor(name: string, stores: string = "No", indexing: string = "Default", sort: string = "None", analyzer: string = 'StandardAnalyzer', suggestionDistance: string = "None", suggestionAccuracy: number = 0.5, termVector: string = "No", public indexFieldNames?:string[]) {
        this.name(name);
        this.stores(stores);
        this.indexing(indexing);
        this.sort(sort);
        this.analyzer(analyzer);
        this.suggestionAccuracy(suggestionAccuracy);
        this.suggestionDistance(suggestionDistance);
        this.termVector(termVector);
        this.name.subscribe(() => this.calculateFieldNamesAutocomplete());
    }

    calculateFieldNamesAutocomplete() {
        if (!!this.indexFieldNames && this.indexFieldNames.length > 0) {
            if (this.name().length > 0) {
                this.fieldNameAutocompletes(this.indexFieldNames.filter((x: string) => x.toLowerCase().indexOf(this.name().toLowerCase()) >= 0));
            } else {
                this.fieldNameAutocompletes(this.indexFieldNames);
            }
        }
    }

    setName(curName: string) {
        this.name(curName);
    }

    toSuggestionDto(): spatialIndexSuggestionDto {
        return {
            Distance: this.suggestionDistance(),
            Accuracy: this.suggestionAccuracy()
        };
    }
}

export = luceneField; 
