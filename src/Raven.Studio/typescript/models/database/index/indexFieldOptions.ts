/// <reference path="../../../../typings/tsd.d.ts"/>

import spatialOptions = require("models/database/index/spatialOptions");

function labelMatcher<T>(labels: Array<valueAndLabelItem<T, string>>): (arg: T) => string {
    return(arg) => labels.find(x => x.value === arg).label;
}

function yesNoLabelProvider(arg: boolean) {
    return arg ? 'Yes' : 'No';
}

class indexFieldOptions {

    static readonly DefaultFieldOptions = "__all_fields";

    static readonly TermVectors: Array<valueAndLabelItem<Raven.Client.Documents.Indexes.FieldTermVector, string>> = [{
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

    static readonly Indexing: Array<valueAndLabelItem<Raven.Client.Documents.Indexes.FieldIndexing, string>> = [
        {
            label: "Default",
            value: "Default"
        }, {
            label: "No",
            value: "No"
            
        }, {
            label: "Exact",
            value: "Exact"
            
        }, {
            label: "Search",
            value: "Search"
        }];
    
    static readonly SpatialType: Array<Raven.Client.Documents.Indexes.Spatial.SpatialFieldType> = ["Cartesian", "Geography"];
    
    static readonly CircleRadiusType: Array<Raven.Client.Documents.Indexes.Spatial.SpatialUnits> = [ "Kilometers", "Miles"];

    name = ko.observable<string>();

    parent = ko.observable<indexFieldOptions>();

    analyzer = ko.observable<string>();

    indexing = ko.observable<Raven.Client.Documents.Indexes.FieldIndexing>();
    effectiveIndexing = this.effectiveComputed(x => x.indexing(), labelMatcher(indexFieldOptions.Indexing));

    storage = ko.observable<Raven.Client.Documents.Indexes.FieldStorage>();
    effectiveStorage = this.effectiveComputed(x => x.storage());

    suggestions = ko.observable<boolean>();
    effectiveSuggestions = this.effectiveComputed(x => x.suggestions(), yesNoLabelProvider);

    termVector = ko.observable<Raven.Client.Documents.Indexes.FieldTermVector>();
    effectiveTermVector = this.effectiveComputed(x => x.termVector(), labelMatcher(indexFieldOptions.TermVectors));

    fullTextSearch = ko.observable<boolean>();
    effectiveFullTextSearch = this.effectiveComputed(x => x.fullTextSearch(), yesNoLabelProvider);

    spatial = ko.observable<spatialOptions>();

    hasSpatialOptions = ko.observable<boolean>(false);
    showAdvancedOptions = ko.observable<boolean>(false);
    canProvideAnalyzer = ko.pureComputed(() => this.indexing() === "Search");

    validationGroup: KnockoutObservable<any>;

    constructor(name: string, dto: Raven.Client.Documents.Indexes.IndexFieldOptions, parentFields?: indexFieldOptions) {
        this.name(name);
        this.parent(parentFields);
        this.analyzer(dto.Analyzer);
        this.indexing(dto.Indexing);
        this.storage(dto.Storage);
        this.suggestions(dto.Suggestions);
        this.termVector(dto.TermVector);
        this.hasSpatialOptions(!!dto.Spatial);
        if (this.hasSpatialOptions()) {
            this.spatial(new spatialOptions(dto.Spatial));
        } else {
            this.spatial(spatialOptions.empty());
        }
        if (this.indexing() === "Search" && !this.analyzer()) {
            this.fullTextSearch(true);
        }

        _.bindAll(this, "toggleAdvancedOptions");

        this.initValidation();

        // used to avoid circular updates
        let fullTextChangeInProgress = false;
        let indexingChangeInProgess = false;

        const onFullTextChanged = () => {
            if (!indexingChangeInProgess) {
                const newValue = this.fullTextSearch();
                fullTextChangeInProgress = true;
                if (newValue) {
                    this.analyzer(null);
                    this.indexing("Search");
                } else {
                    this.analyzer(null);
                    this.indexing("Default");
                }
                fullTextChangeInProgress = false;
            }
        };

        this.fullTextSearch.subscribe(() => onFullTextChanged());

        this.indexing.subscribe(newIndexing => {
            if (!fullTextChangeInProgress) {
                indexingChangeInProgess = true;
                this.fullTextSearch(newIndexing === "Search" && !this.fullTextSearch() && !this.analyzer());
                indexingChangeInProgess = false;
            }
        });

        this.analyzer.subscribe(newAnalyzer => {
            if (!fullTextChangeInProgress) {
                indexingChangeInProgess = true;
                this.fullTextSearch(!newAnalyzer && !this.fullTextSearch() && this.indexing() == "Search");
                indexingChangeInProgess = false;
            }
        });
    }

    private effectiveComputed<T>(extractor: (field: indexFieldOptions) => T, labelProvider?: (arg: T) => string): KnockoutComputed<string> {
        return ko.pureComputed(() => this.extractEffectiveValue(x => extractor(x), true, labelProvider));
    }

    private extractEffectiveValue<T>(extractor: (field: indexFieldOptions) => T, wrapWithDefault: boolean, labelProvider?: (arg: T) => string): string {
        const candidates = [] as T[];

        let field = this as indexFieldOptions;

        while (field) {
            candidates.push(extractor(field));
            field = field.parent();
        }

        const index = candidates.findIndex(x => !_.isNull(x) && !_.isUndefined(x));
        const value = candidates[index];

        const label = labelProvider ? labelProvider(value) : value;

        return (index > 0 && wrapWithDefault) ? "Default" : <any>label;
    }

    private initValidation() {
        if (!this.isDefaultOptions()) {
            this.name.extend({ required: true });
        }

        this.validationGroup = ko.validatedObservable({
            name: this.name
        });
    }
    
    static defaultFieldOptions() {
        return new indexFieldOptions(indexFieldOptions.DefaultFieldOptions, indexFieldOptions.getDefaultDto(), indexFieldOptions.globalDefaults());
    }

    static empty() {
        return new indexFieldOptions("", indexFieldOptions.getDefaultDto(), indexFieldOptions.globalDefaults());
    }

    static globalDefaults() {
        const field = new indexFieldOptions("", {
            Storage: "No",
            Indexing: "Default",
            Analyzer: "StandardAnalyzer",
            Suggestions: false,
            Spatial: null as Raven.Client.Documents.Indexes.Spatial.SpatialOptions,
            TermVector: "No"
        });
        field.fullTextSearch(false);

        return field;
    }

    private static getDefaultDto() {
        return {
            Storage: null,
            Indexing: null,
            Sort: null,
            Analyzer: null,
            Suggestions: null,
            Spatial: null as Raven.Client.Documents.Indexes.Spatial.SpatialOptions,
            TermVector: null
        } as Raven.Client.Documents.Indexes.IndexFieldOptions;
    }

    toggleAdvancedOptions() {
        this.showAdvancedOptions(!this.showAdvancedOptions());
    }

    isDefaultOptions(): boolean {
        return this.name() === indexFieldOptions.DefaultFieldOptions;
    }

    toDto(): Raven.Client.Documents.Indexes.IndexFieldOptions {
        return {
            Analyzer: this.analyzer(),
            Indexing: this.indexing(),
            Storage: this.storage(),
            Suggestions: this.suggestions(),
            TermVector: this.termVector(),
            Spatial: this.hasSpatialOptions() ? this.spatial().toDto() : undefined
        }
    }

}

export = indexFieldOptions; 
