/// <reference path="../../../../typings/tsd.d.ts"/>
import spatialOptions = require("models/database/index/spatialOptions");
import jsonUtil = require("common/jsonUtil");

function labelMatcher<T>(labels: Array<valueAndLabelItem<T, string>>): (arg: T) => string {
    return(arg) => labels.find(x => x.value === arg).label;
}

function yesNoLabelProvider(arg: boolean) {
    return arg ? "Yes" : "No";
}

interface analyzerName {
    shortName: string;
    fullName: string;
}

class indexFieldOptions {

    static readonly analyzersNamesDictionary: analyzerName[] = [
        // default analyzer for indexing.Exact
        { shortName: "Keyword Analyzer", fullName: "KeywordAnalyzer" },
        
        // default analyzer for indexing.Default or when 'index fields options' are not defined
        { shortName: "LowerCase Keyword Analyzer", fullName: "Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers.LowerCaseKeywordAnalyzer" },
        
        { shortName: "LowerCase Whitespace Analyzer", fullName: "LowerCaseWhitespaceAnalyzer" },
        { shortName: "NGram Analyzer", fullName:"NGramAnalyzer" },
        { shortName: "Simple Analyzer", fullName: "SimpleAnalyzer" },
        
        // default analyzer for indexing.Search
        { shortName: "Standard Analyzer", fullName: "StandardAnalyzer" },
        
        { shortName: "Stop Analyzer", fullName: "StopAnalyzer" },
        { shortName: "Whitespace Analyzer", fullName:"WhitespaceAnalyzer" }
        
    ];

    static readonly analyzersNames = indexFieldOptions.analyzersNamesDictionary.map(a => a.shortName)
        // exclude the default analyzer from dropdown list (shown only for Indexing.Default is selected)
        .filter(x => x != "LowerCase Keyword Analyzer");

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
    
    isDefaultFieldOptions = ko.pureComputed(() => this.name() === indexFieldOptions.DefaultFieldOptions);
    
    parent = ko.observable<indexFieldOptions>();

    analyzer = ko.observable<string>();
    disabledAnalyzerText = ko.observable<string>();
    
    isDefaultAnalyzer = ko.pureComputed(() => this.analyzer() === "LowerCase Keyword Analyzer" ||
                                              this.analyzer() === "Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers.LowerCaseKeywordAnalyzer");
    
    // show analyzer only if Indexing.Search defined -or- if analyzer is defined
    showAnalyzer = ko.pureComputed(() => this.indexing() === "Search" ||
                                         (this.indexing() === null && this.parent().indexing() === "Search") ||
                                         this.analyzer());
    
    indexing = ko.observable<Raven.Client.Documents.Indexes.FieldIndexing>();
    effectiveIndexing = this.effectiveComputed(x => x.indexing(), labelMatcher(indexFieldOptions.Indexing));
    defaultIndexing = this.defaultComputed(x => x.indexing(), labelMatcher(indexFieldOptions.Indexing));

    storage = ko.observable<Raven.Client.Documents.Indexes.FieldStorage>();
    effectiveStorage = this.effectiveComputed(x => x.storage());
    defaultStorage = this.defaultComputed(x => x.storage());

    suggestions = ko.observable<boolean>();
    effectiveSuggestions = this.effectiveComputed(x => x.suggestions(), yesNoLabelProvider);
    defaultSuggestions = this.defaultComputed(x => x.suggestions(), yesNoLabelProvider);

    termVector = ko.observable<Raven.Client.Documents.Indexes.FieldTermVector>();
    effectiveTermVector = this.effectiveComputed(x => x.termVector(), labelMatcher(indexFieldOptions.TermVectors));
    defaultTermVector = this.defaultComputed(x => x.termVector(), labelMatcher(indexFieldOptions.TermVectors));

    fullTextSearch = ko.observable<boolean>();
    effectiveFullTextSearch = this.effectiveComputed(x => x.fullTextSearch(), yesNoLabelProvider);
    defaultFullTextSearch = this.defaultComputed(x => x.fullTextSearch(), yesNoLabelProvider);

    highlighting = ko.observable<boolean>();
    effectiveHighlighting = this.effectiveComputed(x => x.highlighting(), yesNoLabelProvider);
    defaultHighlighting = this.defaultComputed(x => x.highlighting(), yesNoLabelProvider);

    spatial = ko.observable<spatialOptions>();
    hasSpatialOptions = ko.observable<boolean>(false);
    
    showAdvancedOptions = ko.observable<boolean>(false);
    explainIndexingStatus: KnockoutComputed<boolean>;

    validationGroup: KnockoutObservable<any>;
    dirtyFlag: () => DirtyFlag;
    
    constructor(name: string, dto: Raven.Client.Documents.Indexes.IndexFieldOptions, parentFields?: indexFieldOptions) {
        this.name(name);
        this.parent(parentFields);
        
        this.analyzer(dto.Analyzer);
        if (this.isDefaultAnalyzer()) {
            this.analyzer("LowerCase Keyword Analyzer"); // show short name in ui
        }
        
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
        
        this.computeFullTextSearch();
        this.computeHighlighting();
        
        if ((this.termVector() && this.termVector() !== "No") ||
            (this.indexing() && this.indexing() !== "Default") ||
            this.analyzer()) {
            this.showAdvancedOptions(true);
        }
        
        _.bindAll(this, "toggleAdvancedOptions");

        this.initValidation();
        this.initObservables();
    }
    
    private initObservables() {
        // used to avoid circular updates
        let changeInProgess = false;

        this.fullTextSearch.subscribe(() => {
            if (!changeInProgess) {
                const newValue = this.fullTextSearch();
                
                changeInProgess = true;
                
                switch (newValue) {
                    case true:
                        this.indexing("Search");
                        this.showAdvancedOptions(true);
                        break;
                    case false:
                        this.indexing("Default");
                        break;
                    case null:
                        if (this.parent().fullTextSearch()) {
                            this.indexing("Search");
                            this.showAdvancedOptions(true);
                        } else {
                            this.indexing("Default");
                        }
                        break;
                }
                
                this.computeAnalyzer();
                this.computeHighlighting();
                
                changeInProgess = false;
            }
        });

        this.highlighting.subscribe(() => {
            if (!changeInProgess) {
                const newValue = this.highlighting();

                changeInProgess = true;
                
                if (newValue) {
                    this.storage("Yes");
                    this.indexing("Search");
                    this.termVector("WithPositionsAndOffsets");
                } else if (newValue === null) {
                    this.storage(null);
                    this.indexing(null);
                    this.termVector(null);
                } else {
                    this.storage("No");
                    this.indexing("Default");
                    this.termVector("No");
                }
                
                this.computeAnalyzer();
                this.computeFullTextSearch();
                changeInProgess = false;
            }
        });
        
        this.indexing.subscribe(() => {
            if (!changeInProgess) {
                changeInProgess = true;
                this.computeAnalyzer();
                this.computeFullTextSearch();
                this.computeHighlighting();
                changeInProgess = false;
            }
        });

        this.analyzer.subscribe(() => {
            if (!changeInProgess) {
                changeInProgess = true;
                this.computeFullTextSearch();
                this.computeHighlighting();
                changeInProgess = false;
            }
        });
        
        this.storage.subscribe(() => {
            if (!changeInProgess) {
                changeInProgess = true;
                this.computeFullTextSearch();
                this.computeHighlighting();
                changeInProgess = false;
            }
        });

        this.termVector.subscribe(() => {
            if (!changeInProgess) {
                changeInProgess = true;
                this.computeFullTextSearch();
                this.computeHighlighting();
                changeInProgess = false;
            }
        });

        this.dirtyFlag = new ko.DirtyFlag([
            this.name,
            this.analyzer,
            this.indexing,
            this.storage,
            this.suggestions,
            this.termVector,
            this.hasSpatialOptions,
            this.spatial().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);

        this.explainIndexingStatus = ko.pureComputed(() => {
           // This case can result from defining an index outside of Studio, where Analyzer is defined but Indexing is Not defined.
           // In this case the server uses Indexing.Search under the hood, even though we get Indexing.Default from the server.
           return (!!this.analyzer() &&
                   !this.isDefaultAnalyzer() &&
                   this.indexing() === null && 
                   (this.parent().indexing() === null || this.parent().indexing() === "Default"));
        });
    }

    private computeFullTextSearch() {
        let fts = false;
        
        switch (this.indexing()) {
            case "Search":
                fts = true;
                break;
            // 'Exact', 'No' & 'Default' stay false
            case null:
                if (!this.analyzer()) {
                    fts = null;
                } else {
                    switch (this.parent().indexing()) {
                        case "Search":
                            fts = true;
                            break;
                        // 'Exact' & 'No' stay false
                        case "Default":
                        case null:
                            if (!this.isDefaultAnalyzer()) {
                                fts = true;
                            }
                            break;
                    }
                }
                break;
        }
        
        this.fullTextSearch(fts);
    }

    private computeHighlighting() {
        this.highlighting(!this.analyzer() &&
                           this.indexing() === "Search" &&
                           this.storage() === "Yes" && 
                           this.termVector() === "WithPositionsAndOffsets");
       
        if (this.storage() === null &&
            this.termVector() === null) {
            this.highlighting(null);
        }
    }
    
    public computeAnalyzer() {
        const thisIndexing = this.indexing();
        const parentIndexing = this.parent().indexing();

        if (thisIndexing === "No" ||
           (thisIndexing === null && parentIndexing === "No")) {
            this.analyzer(null);
        }
        
        this.disabledAnalyzerText("");
        const helpMsg = "To set a different analyzer, select the 'Indexing.Search' option first."
        
        if (thisIndexing === "Exact" ||
           (thisIndexing === null && parentIndexing === "Exact")) {
            this.analyzer("KeywordAnalyzer"); 
            this.disabledAnalyzerText("KeywordAnalyzer is used when selecting Indexing.Exact. " + helpMsg);
        } 
        
        if (thisIndexing === "Default" ||
           (thisIndexing === null && parentIndexing === "Default") ||
           (thisIndexing === null && parentIndexing === null)) {
            this.analyzer("LowerCase Keyword Analyzer");
            this.disabledAnalyzerText("LowerCaseKeywordAnalyzer is used when selecting Indexing.Default. " + helpMsg);
        }
        
        if (thisIndexing === "Search" ||
            (thisIndexing === null && parentIndexing === "Search"))
        {
            this.analyzer("StandardAnalyzer");
        }
    }
    
    private effectiveComputed<T>(extractor: (field: indexFieldOptions) => T, labelProvider?: (arg: T) => string): KnockoutComputed<string> {
        return ko.pureComputed(() => this.extractEffectiveValue(x => extractor(x), true, labelProvider));
    }

    private defaultComputed<T>(extractor: (field: indexFieldOptions) => T, labelProvider?: (arg: T) => string): KnockoutComputed<string> {
        return ko.pureComputed(() => "Inherit (" + this.parent().extractEffectiveValue(x => extractor(x), false, labelProvider) + ")");
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

        return (index > 0 && wrapWithDefault) ? "Inherit (" + label + ")" : <any>label;
    }

    private initValidation() {
        if (!this.isDefaultOptions()) {
            this.name.extend({required: true});
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
        field.highlighting(false);

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
        const analyzer = indexFieldOptions.analyzersNamesDictionary.find(x => x.shortName === this.analyzer()); 
        const analyzerFullName = analyzer ? analyzer.fullName : (this.analyzer() || null);
        
        return {
            Analyzer: analyzerFullName, 
            Indexing: this.indexing(),
            Storage: this.storage(),
            Suggestions: this.suggestions(),
            TermVector: this.termVector(),
            Spatial: this.hasSpatialOptions() ? this.spatial().toDto() : undefined
        }
    }
}

export = indexFieldOptions;
