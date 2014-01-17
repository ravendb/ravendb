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

    private parseFields(): luceneField[] {
        var propOrDefault = <T>(obj: any, prop: string, defaultValue: T) => ko.observable<T>(!obj || !obj[prop] ? defaultValue : obj[prop]);

        return this.fields()
            .filter(name => this.analyzers[name] != null || this.indexes[name] != null || this.sortOptions[name] != null || this.stores[name] != null || this.suggestions[name] != null || this.termVectors[name] != null) // A field is configured and shows up in the index edit UI as a field when it appears in one of the aforementioned objects.
            .map<luceneField>(fieldName => {
                var suggestion = this.suggestions && this.suggestions[fieldName] ? this.suggestions[fieldName] : null;
                return {
                    name: ko.observable(fieldName),
                    analyzer: propOrDefault<string>(this.analyzers, fieldName, null),
                    indexing: propOrDefault<string>(this.indexes, fieldName, "Default"),
                    sort: propOrDefault<string>(this.sortOptions, fieldName, "None"),
                    stores: propOrDefault<string>(this.stores, fieldName, "No"),
                    suggestionDistance: propOrDefault<string>(suggestion, 'Distance', 'None'),
                    suggestionAccuracy: propOrDefault<number>(suggestion, 'Accuracy', 0.5),
                    termVector: propOrDefault(this.termVectors, fieldName, "No")
                };
            });        
    }
}

export = indexDefinition;