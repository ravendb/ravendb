interface luceneField {
    name: KnockoutObservable<string>;
    stores: KnockoutObservable<string>;
    indexing: KnockoutObservable<string>;
    sort: KnockoutObservable<string>;
    analyzer: KnockoutObservable<string>;
    suggestionDistance: KnockoutObservable<string>;
    suggestionAccuracy: KnockoutObservable<number>;
    termVector: KnockoutObservable<string>;
}