class analyzerListItemModel {

    definition: Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition;

    overrideServerWide = ko.observable<boolean>(false);

    constructor(dto: Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition) {
        this.definition = dto;
    }

    get name() {
        return this.definition.Name;
    }
}

export = analyzerListItemModel;
