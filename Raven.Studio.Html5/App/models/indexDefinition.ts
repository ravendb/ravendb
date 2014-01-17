import luceneField = require("models/luceneField");
import spatialIndexField = require("models/spatialIndexField");

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
    spatialFields = ko.observableArray<spatialIndexField>();

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
        this.spatialFields(this.parseSpatialFields());

        // If the a spatial's strategy is changed, we may need to reset to the default maxTreeLevel.
        this.spatialFields().forEach(f => f.strategy.subscribe(newStrategy => {
            if (newStrategy === "GeohashPrefixTree") {
                f.maxTreeLevel(9);
            } else if (newStrategy === "QuadPrefixTree") {
                f.maxTreeLevel(23);
            }
        }));
    }

    private parseFields(): luceneField[] {
        return this.fields()
            .filter(name => this.analyzers[name] != null || this.indexes[name] != null || this.sortOptions[name] != null || this.stores[name] != null || this.suggestions[name] != null || this.termVectors[name] != null) // A field is configured and shows up in the index edit UI as a field when it appears in one of the aforementioned objects.
            .map(fieldName => {
                var suggestion: any = this.suggestions && this.suggestions[fieldName] ? this.suggestions[fieldName] : {};
                return new luceneField(fieldName, this.stores[fieldName], this.indexes[fieldName], this.sortOptions[fieldName], this.analyzers[fieldName], suggestion['Distance'], suggestion['Accuracy'], this.termVectors[fieldName]);
            });        
    }

    private parseSpatialFields(): spatialIndexField[] {
        // The spatial fields are stored as properties on the .spatialIndexes object.
        // The property names will be one of the .fields.
        return this.fields()
            .filter(fieldName => this.spatialIndexes && this.spatialIndexes[fieldName])
            .map(fieldName => new spatialIndexField(fieldName, this.spatialIndexes[fieldName]));
    }
}

export = indexDefinition;