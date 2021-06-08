/// <reference path="../../../../typings/tsd.d.ts"/>
import spatialOptions = require("models/database/index/spatialOptions");
import jsonUtil = require("common/jsonUtil");

function labelMatcher<T>(labels: Array<valueAndLabelItem<T, string>>): (arg: T) => string {
    return(arg) => labels.find(x => x.value === arg).label;
}

function yesNoLabelProvider(arg: boolean) {
    return arg ? "Yes" : "No";
}

type indexingTypes = Raven.Client.Documents.Indexes.FieldIndexing | "Search (implied)";

type preDefinedAnalyzerNameForUI = "Keyword Analyzer" | "LowerCase Keyword Analyzer" | "LowerCase Whitespace Analyzer" |
                                   "NGram Analyzer" | "Simple Analyzer" | "Standard Analyzer" | "Stop Analyzer" | "Whitespace Analyzer";

interface analyzerName {
    studioName: preDefinedAnalyzerNameForUI | string;
    serverName: string;
}

class indexFieldOptions {
    analyzersNamesDictionary = ko.observableArray<analyzerName>([
        // default analyzer for Indexing.Exact
        { studioName: "Keyword Analyzer", serverName: "KeywordAnalyzer" },
        
        // default analyzer for Indexing.Default or when 'Indexing' options are not defined
        { studioName: "LowerCase Keyword Analyzer", serverName: "Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers.LowerCaseKeywordAnalyzer" },
        
        { studioName: "LowerCase Whitespace Analyzer", serverName: "LowerCaseWhitespaceAnalyzer" },
        { studioName: "NGram Analyzer", serverName:"NGramAnalyzer" },
        { studioName: "Simple Analyzer", serverName: "SimpleAnalyzer" },
        
        // default analyzer for Indexing.Search
        { studioName: "Standard Analyzer", serverName: "StandardAnalyzer" },
        
        { studioName: "Stop Analyzer", serverName: "StopAnalyzer" },
        { studioName: "Whitespace Analyzer", serverName:"WhitespaceAnalyzer" }
    ]);
    
    analyzersNames = ko.pureComputed(() => {
        return this.analyzersNamesDictionary().map(a => a.studioName)
            // exclude the default analyzer from dropdown list (shown only when Indexing.Default is selected)
            .filter(x => x !== "LowerCase Keyword Analyzer");
    })

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

    static readonly IndexingWithSearchImplied: Array<valueAndLabelItem<indexingTypes, string>> =
        [...indexFieldOptions.Indexing, { label: "Search (implied)", value: "Search (implied)" }];
    
    static readonly SpatialType: Array<Raven.Client.Documents.Indexes.Spatial.SpatialFieldType> = ["Cartesian", "Geography"];
    
    static readonly CircleRadiusType: Array<Raven.Client.Documents.Indexes.Spatial.SpatialUnits> = [ "Kilometers", "Miles"];

    name = ko.observable<string>();
    
    isDefaultFieldOptions = ko.pureComputed(() => this.name() === indexFieldOptions.DefaultFieldOptions);
    
    parent = ko.observable<indexFieldOptions>();

    analyzer = ko.observable<string>();
    disabledAnalyzerText = ko.observable<string>();
    analyzerPlaceHolder = ko.observable<string>();
    
    analyzerDefinedWithoutIndexing = ko.observable<boolean>(false);
    theAnalyzerThatWasDefinedWithoutIndexing = ko.observable<string>();

    isDefaultAnalyzer: KnockoutComputed<boolean>;
    showAnalyzer: KnockoutComputed<boolean>;

    indexing = ko.observable<indexingTypes>(); // the actual value
    effectiveIndexing = this.effectiveComputed(x => x.indexing(), labelMatcher(indexFieldOptions.IndexingWithSearchImplied)); // for button label
    defaultIndexing = this.defaultComputed(x => x.indexing(), labelMatcher(indexFieldOptions.IndexingWithSearchImplied)); // for dropdown label
    indexingDropdownOptions: KnockoutComputed<Array<valueAndLabelItem<indexingTypes, string>>>;

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

    indexOrStore: KnockoutComputed<boolean>;
    
    showAdvancedOptions = ko.observable<boolean>(false);

    validationGroup: KnockoutObservable<any>;
    dirtyFlag: () => DirtyFlag;
    
    constructor(name: string, dto: Raven.Client.Documents.Indexes.IndexFieldOptions, parentFields?: indexFieldOptions) {
        this.name(name);
        this.parent(parentFields);
        
        const analyzerPositionInName = dto.Analyzer ? dto.Analyzer.lastIndexOf(".") : 0;
        const analyzerNameInDto = analyzerPositionInName !== -1 && dto.Analyzer ? dto.Analyzer.substring(analyzerPositionInName + 1) : dto.Analyzer;
        const analyzerInDictionary = this.analyzersNamesDictionary().find(x => x.serverName === analyzerNameInDto);
        
        let analyzerNameForStudio = null;
        
        if (analyzerInDictionary) {
            // analyzer is one of our pre-defined analyzers
            analyzerNameForStudio = analyzerInDictionary.studioName;
        } else if (dto.Analyzer) {
            // analyzer is a custom analyzer, add it to the names dictionary
            this.analyzersNamesDictionary.push({ studioName: dto.Analyzer, serverName: dto.Analyzer });
            analyzerNameForStudio = dto.Analyzer;
        }
        
        this.analyzer(analyzerNameForStudio);
        
        this.isDefaultAnalyzer = ko.pureComputed(() => this.analyzer() === "LowerCase Keyword Analyzer" ||
                                                       this.analyzer() === "Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers.LowerCaseKeywordAnalyzer" ||
                                                       this.analyzerPlaceHolder() === "LowerCase Keyword Analyzer");
        
        this.showAnalyzer = ko.pureComputed(() => this.indexing() === "Search" ||
                                                   this.indexing() === "Search (implied)" ||
                                                  (!this.indexing() && this.parent().indexing() === "Search") ||
                                                  !!this.analyzer() ||
                                                  (!this.analyzer() && !!this.analyzerPlaceHolder()));
        
        if (this.isDefaultAnalyzer()) {
            this.analyzer(null);
        }
        
        this.indexing(dto.Indexing);
        
        // for issue RavenDB-12607
        if (!dto.Indexing && this.analyzer() && !this.isDefaultAnalyzer()) {
           this.analyzerDefinedWithoutIndexing(true);
           this.theAnalyzerThatWasDefinedWithoutIndexing(this.analyzer());
           this.indexing("Search (implied)");
        }
        
        this.storage(dto.Storage);
        this.suggestions(dto.Suggestions);
        this.termVector(dto.TermVector);
        this.hasSpatialOptions(!!dto.Spatial);
        
        if (this.hasSpatialOptions()) {
            this.spatial(new spatialOptions(dto.Spatial));
        } else {
            this.spatial(spatialOptions.empty());
        }

        this.computeAnalyzer();
        this.computeFullTextSearch();
        this.computeHighlighting();
        
        _.bindAll(this, "toggleAdvancedOptions");

        this.initObservables();
        this.initValidation();

        if ((this.termVector() && this.termVector() !== "No") ||
            (this.indexing() && this.indexing() !== "Default")) {
            this.showAdvancedOptions(true);
        }
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

        this.indexOrStore = ko.pureComputed(() => {
            return !(this.indexing() === "No" && this.effectiveStorage().includes("No"));
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

        this.parent.subscribe(() => {
            if (!changeInProgess) {
                changeInProgess = true;
                if (!this.isDefaultFieldOptions()) {
                    this.computeAnalyzer();
                    this.computeFullTextSearch();
                    this.computeHighlighting();
                }
                changeInProgess = false;
            }
        });
        
        this.indexingDropdownOptions = ko.pureComputed(() => {
           return this.analyzerDefinedWithoutIndexing() ? indexFieldOptions.IndexingWithSearchImplied : indexFieldOptions.Indexing; 
        });
    }

    private computeFullTextSearch() {
        let fts = false;
        
        switch (this.indexing()) {
            case "Search":
            case "Search (implied)":
                fts = true;
                break;
            // 'Exact', 'No' & 'Default' stay false
            case null:
                if (!this.analyzer() && !this.analyzerPlaceHolder()) {
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
        this.highlighting(!this.analyzer() && this.analyzerPlaceHolder() &&
                          (this.indexing() === "Search" || this.indexing() === "Search (implied)") &&
                           this.storage() === "Yes" &&
                           this.termVector() === "WithPositionsAndOffsets");
       
        if (this.storage() === null &&
            this.termVector() === null) {
            this.highlighting(null);
        }
    }
    
    public computeAnalyzer() {
        let placeHolder = null;
        const thisIndexing = this.indexing();
        const parentIndexing = this.parent() ? this.parent().indexing() : null;

        if (thisIndexing === "No" ||
           (!thisIndexing && parentIndexing === "No")) {
            this.analyzer(null);
        }
        
        this.disabledAnalyzerText("");
        const helpMsg = "To set a different analyzer, select the 'Indexing.Search' option first."
        
        if (thisIndexing === "Exact" ||
           (!thisIndexing && parentIndexing === "Exact")) {
            this.analyzer(null);
            placeHolder = "Keyword Analyzer";
            this.disabledAnalyzerText("KeywordAnalyzer is used when selecting Indexing.Exact. " + helpMsg);
        } 
        
        if (thisIndexing === "Default" ||
           (!thisIndexing && (parentIndexing === "Default" || !parentIndexing))) {
            this.analyzer(null);
            placeHolder = "LowerCase Keyword Analyzer";
            this.disabledAnalyzerText("LowerCaseKeywordAnalyzer is used when selecting Indexing.Default. " + helpMsg);
        }

        if (thisIndexing === "Search (implied)") {
            this.disabledAnalyzerText("Cannot edit analyzer when Search is implied");
        }

        if (!thisIndexing && parentIndexing === "Search") {
            this.analyzer(null);
            placeHolder = this.parent().analyzer() || "Standard Analyzer";
        }

        if (thisIndexing === "Search") {
            placeHolder = "Standard Analyzer";
        }
        
        // for issue RavenDB-12607
        if (thisIndexing === "Search (implied)") {
            this.analyzer(this.theAnalyzerThatWasDefinedWithoutIndexing());
        }

        this.analyzerPlaceHolder(placeHolder);

        if (this.analyzer() || (!this.analyzer() && this.analyzerPlaceHolder())) {
            this.showAdvancedOptions(true);
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

        this.indexOrStore.extend({
            validation: [
                {
                    validator: () => this.indexOrStore(),
                    message: "'Indexing' and 'Store' cannot be set to 'No' at the same time. A field must be either Indexed or Stored."
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            name: this.name,
            indexOrStore: this.indexOrStore
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

    createAnalyzerNameAutocompleter(analyzerName: string): KnockoutComputed<string[]> {
        return ko.pureComputed(() => {
            if (analyzerName) {
                return this.analyzersNames().filter(x => x.toLowerCase().includes(analyzerName.toLowerCase()));
            } else {
                return this.analyzersNames();
            }
        });
    }

    addCustomAnalyzers(customAnalyzers: string[]) {
        const analyzers = this.analyzersNamesDictionary();

        customAnalyzers.forEach(name => {
            if (!this.analyzersNamesDictionary().find(x => x.studioName === name)) {
                const customAnalyzerEntry: analyzerName = { studioName: name, serverName: name };
                analyzers.push(customAnalyzerEntry);
            }
        });
        
        this.analyzersNamesDictionary(analyzers.sort());
    }
    
    toDto(): Raven.Client.Documents.Indexes.IndexFieldOptions {
        let analyzerToSend = null;
        
        if (this.analyzer()) {
            const selectedAnalyzer = this.analyzersNamesDictionary().find(x => x.studioName === this.analyzer());
            analyzerToSend = selectedAnalyzer ? selectedAnalyzer.serverName : this.analyzer();
        }
        
        return {
            Analyzer: analyzerToSend,
            Indexing: this.indexing() === "Search (implied)" ? null : this.indexing() as Raven.Client.Documents.Indexes.FieldIndexing,
            Storage: this.storage(),
            Suggestions: this.suggestions(),
            TermVector: this.termVector(),
            Spatial: this.hasSpatialOptions() ? this.spatial().toDto() : undefined
        }
    }
}

export = indexFieldOptions;
