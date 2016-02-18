/// <reference path="../../../../typings/tsd.d.ts"/>

class luceneField {
    name = ko.observable<string>();
    stores = ko.observable<string>();
    sort = ko.observable<string>();
    termVector = ko.observable<string>();
    indexing = ko.observable<string>();
    analyzer = ko.observable<string>();
    suggestionEnabled = ko.observable<boolean>();
    
    fieldNameAutocompletes = ko.observableArray<string>();

    constructor(name: string, stores: string = "No", indexing: string = "Default", sort: string = "None", analyzer: string = 'StandardAnalyzer', suggestionEnabled: boolean = false, termVector: string = "No", public indexFieldNames?:string[]) {
        this.name(name);
        this.stores(stores);
        this.indexing(indexing);
        this.sort(sort);
        this.analyzer(analyzer);
        this.suggestionEnabled(suggestionEnabled);
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
}

export = luceneField; 
