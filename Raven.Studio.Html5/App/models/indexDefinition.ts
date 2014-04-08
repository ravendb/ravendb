import luceneField = require("models/luceneField");
import spatialIndexField = require("models/spatialIndexField");

class indexDefinition {
    analyzers: any;
    fields = ko.observableArray<string>().extend({ required: true });
    indexes: any;
    internalFieldsMapping: any;
    isCompiled: boolean;
    isMapReduce: boolean;
    lockMode: string;
    map = ko.observable<string>().extend({ required: true });
    maps = ko.observableArray<KnockoutObservable<string>>().extend({ required: true });
    name = ko.observable<string>().extend({ required: true });
    reduce = ko.observable<string>().extend({ required: true });
    sortOptions: any;
    spatialIndexes: any;
    stores: any;
    suggestions: any;
    termVectors: any;
    transformResults = ko.observable<string>().extend({ required: true });
    type: string;
    maxIndexOutputsPerDocument = ko.observable<number>(0);

    // This is an amalgamation of several properties from the index (Fields, Stores, Indexes, SortOptions, Analyzers, Suggestions, TermVectors) 
    // Stored as multiple luceneFields for the sake of data binding.
    // Each luceneField corresponds to a Field box in the index editor UI.
    luceneFields = ko.observableArray<luceneField>();
    spatialFields = ko.observableArray<spatialIndexField>().extend({ required: true });

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
        this.type = dto.Type;

        this.luceneFields(this.parseFields());
        this.spatialFields(this.parseSpatialFields());

        this.maxIndexOutputsPerDocument(dto.MaxIndexOutputsPerDocument? dto.MaxIndexOutputsPerDocument:0);
    }

    toDto(): indexDefinitionDto {
        return {
            Analyzers: this.makeFieldObject(f => f.indexing() === "Analyzed", f => f.analyzer()),
            Fields: this.fields(),
            Indexes: this.makeFieldObject(f => f.indexing() !== "Default", f => f.indexing()),
            InternalFieldsMapping: this.internalFieldsMapping,
            IsCompiled: this.isCompiled,
            IsMapReduce: this.isMapReduce,
            LockMode: this.lockMode,
            Map: this.maps()[0](),
            Maps: this.maps().map(m => m()).filter(m => m && m.length > 0),
            Name: this.name(),
            Reduce: this.reduce(),
            SortOptions: this.makeFieldObject(f => f.sort() !== "None", f => f.sort()),
            SpatialIndexes: this.makeSpatialIndexesObject(),
            Stores: this.makeFieldObject(f => f.stores() === "Yes", f => f.stores()),
            Suggestions: this.makeFieldObject(f => f.suggestionDistance() !== "None", f => f.toSuggestionDto()),
            TermVectors: this.makeFieldObject(f => f.termVector() !== "No", f => f.termVector()),
            TransformResults: this.transformResults(),
            Type: this.type,
            MaxIndexOutputsPerDocument: this.maxIndexOutputsPerDocument()? this.maxIndexOutputsPerDocument()> 0 ? this.maxIndexOutputsPerDocument() :null:null
        };
    }

    static empty(): indexDefinition {
        return new indexDefinition({
            Analyzers: {},
            Fields: [],
            Indexes: {},
            InternalFieldsMapping: {},
            IsCompiled: false,
            IsMapReduce: false,
            LockMode: "Unlock",
            Map: " ",
            Maps: [" "],
            Name: "",
            Reduce: null,
            SortOptions: {},
            SpatialIndexes: {},
            Stores: {},
            Suggestions: {},
            TermVectors: {},
            TransformResults: null,
            Type: "Map",
            MaxIndexOutputsPerDocument:null
        });
    }

    private makeSpatialIndexesObject(): any {
        var spatialIndexesObj = {};
        this.spatialFields().forEach(f => spatialIndexesObj[f.name()] = f.toDto());
        return spatialIndexesObj;
    }

    private makeFieldObject(filter: (field: luceneField) => boolean, selector: (field: luceneField) => any): any {
        var obj = {};
        this.luceneFields()
            .filter(filter)
            .forEach(f => obj[f.name()] = selector(f));
        return obj;
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