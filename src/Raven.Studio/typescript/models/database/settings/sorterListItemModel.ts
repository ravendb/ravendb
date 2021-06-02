class sorterListItem {

    definition: Raven.Client.Documents.Queries.Sorting.SorterDefinition;

    testModeEnabled = ko.observable<boolean>(false);
    testRql = ko.observable<string>();
    
    overrideServerWide = ko.observable<boolean>(false);

    constructor(dto: Raven.Client.Documents.Queries.Sorting.SorterDefinition) {
        this.definition = dto;
        this.testRql(`from index <indexName>\r\norder by custom(<fieldName>, "${dto.Name}")`);
    }

    get name() {
        return this.definition.Name;
    }
}

export = sorterListItem;
