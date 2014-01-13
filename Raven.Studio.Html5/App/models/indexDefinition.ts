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
    }
}

export = indexDefinition;