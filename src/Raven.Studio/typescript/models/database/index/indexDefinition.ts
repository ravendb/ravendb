/// <reference path="../../../../typings/tsd.d.ts"/>

import luceneField = require("models/database/index/luceneField");
import spatialIndexField = require("models/database/index/spatialIndexField");

class indexDefinition {
    name = ko.observable<string>();
    map = ko.observable<string>();
    maps = ko.observableArray<KnockoutObservable<string>>();
    reduce = ko.observable<string>();
    luceneFields = ko.observableArray<luceneField>();
    isTestIndex = ko.observable<boolean>(false);
    isSideBySideIndex = ko.observable<boolean>(false);
    numOfLuceneFields = ko.computed(() => this.luceneFields().length);

    // This is an amalgamation of several properties from the index (Fields, Stores, Indexes, SortOptions, Analyzers, Suggestions, TermVectors) 
    // Stored as multiple luceneFields for the sake of data binding.
    // Each luceneField corresponds to a Field box in the index editor UI.
    spatialFields = ko.observableArray<spatialIndexField>();
    numOfSpatialFields = ko.computed(() => this.spatialFields().length);

    maxIndexOutputsPerDocument = ko.observable<number>(0);
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
    type: Raven.Client.Data.Indexes.IndexType;

    constructor(dto: Raven.Client.Indexing.IndexDefinition) {
        //TODO: this.analyzers = dto.Analyzers;
        //TODO: this.fields(dto.Fields);
        //TODO: this.indexes = dto.Indexes;
        //TODO: this.internalFieldsMapping = dto.InternalFieldsMapping;
        this.isTestIndex(dto.IsTestIndex);
        this.isSideBySideIndex(dto.IsSideBySideIndex);
        //TODO: this.isCompiled = dto.IsCompiled;
        //TODO: this.isMapReduce = dto.IsMapReduce;
        this.lockMode = dto.LockMode;
        //TODO: this.map(dto.Map);
        this.maps(dto.Maps.map(m => ko.observable(m)));
        this.name(dto.Name);
        this.reduce(dto.Reduce);
        //TODO: this.sortOptions = dto.SortOptions;
        //TODO: this.spatialIndexes = dto.SpatialIndexes;
        //TODO: this.stores = dto.Stores;
        //TODO: this.suggestionsOptions = dto.SuggestionsOptions;
        //TODO: this.termVectors = dto.TermVectors;
        this.type = dto.Type;

        this.luceneFields(this.parseFields());
        this.spatialFields(this.parseSpatialFields());

        this.maxIndexOutputsPerDocument(0);
        //TODO this.maxIndexOutputsPerDocument(dto.MaxIndexOutputsPerDocument ? dto.MaxIndexOutputsPerDocument : 0);
        this.storeAllFields(this.isStoreAllFields());
    }

    isStoreAllFields(): boolean {
        if (this.stores && this.stores.hasOwnProperty("__all_fields")) {
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

    toDto(): Raven.Client.Indexing.IndexDefinition {
        return {
            //Analyzers: this.makeFieldObject(f => f.indexing() === "Analyzed", f => f.analyzer()),
            Fields: {},//this.fields(),
            //Indexes: this.makeFieldObject(f => f.indexing() !== "Default", f => f.indexing()),
            //InternalFieldsMapping: this.internalFieldsMapping,
            IsTestIndex: this.isTestIndex(),
            IsSideBySideIndex: this.isSideBySideIndex(),
            //IsCompiled: this.isCompiled,
            //IsMapReduce: this.isMapReduce,
            LockMode: "Unlock", //TODO:
            IndexId: null, 
            //Map: this.maps()[0](),
            Maps: this.maps().map(m => m()).filter(m => m && m.length > 0),
            Name: this.name(),
            Reduce: this.reduce(),
            //SortOptions: this.makeFieldObject(f => f.sort() !== "None", f => f.sort()),
            //SpatialIndexes: this.makeSpatialIndexesObject(),
            //Stores: this.setStoreAllFieldsToObject(this.makeFieldObject(f => f.stores() === "Yes", f => f.stores())),
            //SuggestionsOptions: this.luceneFields().filter(x => x.suggestionEnabled()).map(x => x.name()),
            //TermVectors: this.makeFieldObject(f => f.termVector() !== "No", f => f.termVector()),
            Type: this.type,
            Configuration: null //TODO
            //TODO MaxIndexOutputsPerDocument: this.maxIndexOutputsPerDocument() ? this.maxIndexOutputsPerDocument() > 0 ? this.maxIndexOutputsPerDocument() : null : null
        };
    }

    static empty(): indexDefinition {
        return new indexDefinition({
            Fields: {},
            IndexId: null,
            Maps: [""],
            Name: "",
            LockMode: "Unlock",
            Reduce: "",
            Configuration: null, //TODO
            //TODO IndexVersion: -1,
            IsSideBySideIndex: false,
            IsTestIndex: false,
            //TODO MaxIndexOutputsPerDocument: null,
            Type: "Map"
        });
    }

    private makeSpatialIndexesObject(): any {
        var spatialIndexesObj = {};
        this.spatialFields().forEach(f => (<any>spatialIndexesObj)[f.name()] = f.toDto());
        return spatialIndexesObj;
    }

    private makeFieldObject(filter: (field: luceneField) => boolean, selector: (field: luceneField) => any): any {
        var obj = {};
        this.luceneFields()
            .filter(filter)
            .forEach(f => (<any>obj)[f.name()] = selector(f));
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

        var keys: Array<string> = [];
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
        
        var fields: spatialIndexField[] = [];
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
