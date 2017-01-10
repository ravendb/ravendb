/// <reference path="../../../../typings/tsd.d.ts"/>

import spatialOptions = require("models/database/index/spatialOptions");

class indexFieldOptions {

    static readonly DefaultFieldOptions = "__all_fields";
    static readonly SortOptions: Array<valueAndLabelItem<Raven.Abstractions.Indexing.SortOptions, string>> = [{
            label: "None",
            value: "None"
        }, {
            label: "Numeric (default)",
            value: "NumericDefault"
        }, {
            label: "Numeric (double)",
            value: "NumericDouble"
        }, {
            label: "Numeric (long)",
            value: "NumericLong"
        }, {
            label: "String", 
            value: "String"
        }, {
            label: "StringVal",
            value: "StringVal"
        }];

    static readonly TermVectors: Array<valueAndLabelItem<Raven.Abstractions.Indexing.FieldTermVector, string>> = [{
            label: "No",
            value: "No"
        }, {
            label: "With offsets",
            value: "WithOffsets"
        }, {
            label: "With positions",
            value: "WithPositions"
        }, {
            label: "With positions and offsets",
            value: "WithPositionsAndOffsets"
        }, {
            label: "Yes", 
            value: "Yes"
        }
    ];

    static readonly Indexing: Array<valueAndLabelItem<Raven.Abstractions.Indexing.FieldIndexing, string>> = [{
            label: "Analyzed", 
            value: "Analyzed"
        }, {
            label: "Default",
            value: "Default"
        }, {
            label: "Not analyzed",
            value: "NotAnalyzed"
        }, {
            label: "No",
            value: "No"
    }];

    name = ko.observable<string>();

    analyzer = ko.observable<string>();
    indexing = ko.observable<Raven.Abstractions.Indexing.FieldIndexing>();
    sort = ko.observable<Raven.Abstractions.Indexing.SortOptions>();
    storage = ko.observable<boolean>();
    suggestions = ko.observable<boolean>();
    termVector = ko.observable<Raven.Abstractions.Indexing.FieldTermVector>();
    spatial = ko.observable<spatialOptions>();

    hasSpatialOptions = ko.observable<boolean>(false);
    showAdvancedOptions = ko.observable<boolean>(false);
    canProvideAnalyzer = ko.pureComputed(() => this.indexing() === "Analyzed");

    constructor(name: string, dto: Raven.Client.Indexing.IndexFieldOptions) {
        this.name(name);
        this.analyzer(dto.Analyzer);
        this.indexing(dto.Indexing);
        this.sort(dto.Sort);
        this.storage(dto.Storage === "Yes");
        this.suggestions(dto.Suggestions);
        this.termVector(dto.TermVector);
        this.hasSpatialOptions(!!dto.Spatial);
        if (this.hasSpatialOptions()) {
            this.spatial(new spatialOptions(dto.Spatial));
        } else {
            this.spatial(spatialOptions.empty());
        }

        _.bindAll(this, "toggleAdvancedOptions");
    }

    static empty() {
        return new indexFieldOptions("", {
            Storage: "No",
            Indexing: "Default",
            Sort: "None",
            Analyzer: "StandardAnalyzer",
            Suggestions: false,
            Spatial: null as Raven.Abstractions.Indexing.SpatialOptions, 
            TermVector: "No"
        } as Raven.Client.Indexing.IndexFieldOptions);
    }

    toggleAdvancedOptions() {
        this.showAdvancedOptions(!this.showAdvancedOptions());
    }

    isDefaultOptions(): boolean {
        return this.name() === indexFieldOptions.DefaultFieldOptions;
    }

    toDto(): Raven.Client.Indexing.IndexFieldOptions {
        return {
            Analyzer: this.analyzer(),
            Indexing: this.indexing(),
            Sort: this.sort(),
            Storage: this.storage() ? "Yes" : "No",
            Suggestions: this.suggestions(),
            TermVector: this.termVector(),
            Spatial: this.hasSpatialOptions() ? this.spatial().toDto() : undefined
        }
    }

}

export = indexFieldOptions; 
