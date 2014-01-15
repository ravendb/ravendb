class indexDefinition {
    analyzers: any;
    fields = ko.observableArray<string>();
    indexes: any;
    internalFieldsMapping: any;
    isCompiled: boolean;
    isMapReduce: boolean;
    lockMode: string;
    map = ko.observable<string>();
    maps = ko.observableArray<KnockoutObservable<string>>();
    name = ko.observable<string>();
    reduce = ko.observable<string>();
    sortOptions: any;
    spatialIndexes: any;
    stores: any;
    suggestions: any;
    termVectors: any;
    transformResults = ko.observable<string>();

    // This is an amalgamation of several properties from the index (Fields, Stores, Indexes, SortOptions, Analyzers, Suggestions, TermVectors) 
    // Stored as multiple luceneFields for the sake of data binding.
    // Each luceneField corresponds to a Field box in the index editor UI.
    luceneFields = ko.observableArray<luceneField>();

    constructor(dto: indexDefinitionDto) {
        this.analyzers = dto.Analyzers;
        this.fields(dto.Fields);
        this.indexes = dto.Indexes;
        this.internalFieldsMapping = dto.InternalFieldsMapping;
        this.isCompiled = dto.IsCompiled;
        this.isMapReduce = dto.IsMapReduce;
        this.lockMode = dto.LockMode;
        this.map(dto.Map);
        this.maps(dto.Maps.map(m => ko.observable(m)));
        this.name(dto.Name);
        this.reduce(dto.Reduce);
        this.sortOptions = dto.SortOptions;
        this.spatialIndexes = dto.SpatialIndexes;
        this.stores = dto.Stores;
        this.suggestions = dto.Suggestions;
        this.termVectors = dto.TermVectors;
        this.transformResults(dto.TransformResults);

        this.luceneFields(this.parseFields());
    }

    private parseFields() {
        var luceneFields = this.fields().map<luceneField>(fieldName => {
            return {
                name: fieldName,
                analyzer: this.analyzers ? this.analyzers[fieldName] : null,
                indexing: this.indexes ? this.indexes[fieldName] : "Default",
                sort: this.sortOptions ? this.sortOptions[fieldName] : "None",
                stores: this.stores ? this.stores[fieldName] : "No",
                suggestion: this.suggestions ? this.suggestions[fieldName] : "None",
                termVector: this.termVectors ? this.termVectors[fieldName] : "No"
            };
        });

        // If we have any field (besides name), then it's a Lucene field that will show up in the UI.
        // Otherwise, it's just a field tracked by Raven.
        return luceneFields.filter(f =>
            f.analyzer != null || f.indexing != null || f.sort != null ||
            f.stores != null || f.suggestion != null || f.termVector != null);
    }
}

export = indexDefinition;