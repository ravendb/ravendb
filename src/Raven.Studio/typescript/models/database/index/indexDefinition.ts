/// <reference path="../../../../typings/tsd.d.ts"/>

import luceneField = require("models/database/index/luceneField");
import spatialIndexField = require("models/database/index/spatialIndexField");

class indexDefinition {
    name = ko.observable<string>().extend({ required: true });
    map = ko.observable<string>().extend({ required: true });
    maps = ko.observableArray<KnockoutObservable<string>>().extend({ required: true });
    reduce = ko.observable<string>().extend({ required: true });
    luceneFields = ko.observableArray<luceneField>();
    isTestIndex = ko.observable<boolean>(false);
    isSideBySideIndex = ko.observable<boolean>(false);
    numOfLuceneFields = ko.computed(() => this.luceneFields().length).extend({ required: true });

    // This is an amalgamation of several properties from the index (Fields, Stores, Indexes, SortOptions, Analyzers, Suggestions, TermVectors) 
    // Stored as multiple luceneFields for the sake of data binding.
    // Each luceneField corresponds to a Field box in the index editor UI.
    spatialFields = ko.observableArray<spatialIndexField>();
    numOfSpatialFields = ko.computed(() => this.spatialFields().length).extend({ required: true });

    maxIndexOutputsPerDocument = ko.observable<number>(0).extend({ required: true });
    storeAllFields = ko.observable<boolean>(false);

    analyzers: any;
    fields = ko.observableArray<string>();
    indexes: any;
    internalFieldsMapping: any;
    isCompiled: boolean;
    isMapReduce: boolean;
    lockMode: string;
    sortOptions: any;
    spatialIndexes: any;
    stores: any;
    suggestionsOptions: any[];
    termVectors: any;
    type: string;

    constructor(dto: indexDefinitionDto) {
        this.analyzers = dto.Analyzers;
        this.fields(dto.Fields);
        this.indexes = dto.Indexes;
        this.internalFieldsMapping = dto.InternalFieldsMapping;
        this.isTestIndex(dto.IsTestIndex);
        this.isSideBySideIndex(dto.IsSideBySideIndex);
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
        this.suggestionsOptions = dto.SuggestionsOptions;
        this.termVectors = dto.TermVectors;
        this.type = dto.Type;

        this.luceneFields(this.parseFields());
        this.spatialFields(this.parseSpatialFields());

        this.maxIndexOutputsPerDocument(dto.MaxIndexOutputsPerDocument? dto.MaxIndexOutputsPerDocument:0);
        this.storeAllFields(this.isStoreAllFields());
    }

    isStoreAllFields(): boolean {
        if (this.stores.hasOwnProperty("__all_fields")) {
            return this.stores["__all_fields"] === "Yes";
        }

        return false;
    }

    setOrRemoveStoreAllFields(add: boolean) {
        if (add) {
            this.stores["__all_fields"] = "Yes";
        } else {
            delete this.stores["__all_fields"];
        }

        this.storeAllFields(this.isStoreAllFields());
    }

    setStoreAllFieldsToObject(obj: any): any {
        if (this.isStoreAllFields())
            obj["__all_fields"] = "Yes";
        return obj;
    }

    toDto(): indexDefinitionDto {
        return {
            Analyzers: this.makeFieldObject(f => f.indexing() === "Analyzed", f => f.analyzer()),
            Fields: this.fields(),
            Indexes: this.makeFieldObject(f => f.indexing() !== "Default", f => f.indexing()),
            InternalFieldsMapping: this.internalFieldsMapping,
            IsTestIndex: this.isTestIndex(),
            IsSideBySideIndex: this.isSideBySideIndex(),
            IsCompiled: this.isCompiled,
            IsMapReduce: this.isMapReduce,
            LockMode: this.lockMode,
            Map: this.maps()[0](),
            Maps: this.maps().map(m => m()).filter(m => m && m.length > 0),
            Name: this.name(),
            Reduce: this.reduce(),
            SortOptions: this.makeFieldObject(f => f.sort() !== "None", f => f.sort()),
            SpatialIndexes: this.makeSpatialIndexesObject(),
            Stores: this.setStoreAllFieldsToObject(this.makeFieldObject(f => f.stores() === "Yes", f => f.stores())),
            SuggestionsOptions: this.luceneFields().filter(x => x.suggestionEnabled()).map(x => x.name()),
            TermVectors: this.makeFieldObject(f => f.termVector() !== "No", f => f.termVector()),
            Type: this.type,
            MaxIndexOutputsPerDocument: this.maxIndexOutputsPerDocument() ? this.maxIndexOutputsPerDocument()> 0 ? this.maxIndexOutputsPerDocument() : null : null
        };
    }

    static empty(): indexDefinition {
        return new indexDefinition({
            Analyzers: {},
            Fields: [],
            Indexes: {},
            InternalFieldsMapping: {},
            IsTestIndex: false,
            IsSideBySideIndex: false,
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
            SuggestionsOptions: [],
            TermVectors: {},
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
        var fieldSources = [
            this.analyzers,
            this.indexes,
            this.sortOptions,
            this.stores,
            this.suggestionsOptions,
            this.termVectors
        ];

        var keys = [];
        for (var i = 0; i < fieldSources.length; i++) {
            var src = fieldSources[i];
            if (src == null)
                continue;
            for (var key in src) {
                if (src.hasOwnProperty(key) === true && !keys.contains(key))
                    keys.push(key);
            }
        }

        return keys
            .map(fieldName => {
                var suggestionsEnabled = this.suggestionsOptions && this.suggestionsOptions.indexOf(fieldName) >= 0;
                return new luceneField(fieldName, this.stores[fieldName], this.indexes[fieldName], this.sortOptions[fieldName], this.analyzers[fieldName], suggestionsEnabled, this.termVectors[fieldName], this.fields());
            });        
    }

    private parseSpatialFields(): spatialIndexField[] {
        // The spatial fields are stored as properties on the .spatialIndexes object.
        // The property names will be one of the .fields.
        
        var fields = [];
        if (this.spatialIndexes == null)
            return fields;

        for (var key in this.spatialIndexes) {
            var spatialIndex = this.spatialIndexes[key];
            fields.push(new spatialIndexField(key, spatialIndex));
        }
        return fields;
    }
}

export = indexDefinition;
