import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getIndexDefinitionCommand = require("commands/getIndexDefinitionCommand");

class reporting extends viewModelBase {
    selectedIndexName = ko.observable<string>();
    selectedIndexLabel = ko.computed(() => this.selectedIndexName() ? this.selectedIndexName() : "[Select an index]");
    indexNames = ko.observableArray<string>();
    hasSelectedIndex = ko.computed(() => this.selectedIndexName() && this.selectedIndexName().length > 0);
    editSelectedIndexUrl = ko.computed(() => this.hasSelectedIndex() ? appUrl.forEditIndex(this.selectedIndexName(), this.activeDatabase()) : null);
    availableFields = ko.observableArray<string>();
    selectedField = ko.observable<string>();
    selectedFieldLabel = ko.computed(() => this.selectedField() ? this.selectedField() : "Select a field");
    addedValues = ko.observableArray<facetDto>();

    activate(indexToActivateOrNull: string) {
        super.activate(indexToActivateOrNull);

        this.fetchIndexes()
            .done(() => this.selectInitialIndex(indexToActivateOrNull));
    }

    fetchIndexes(): JQueryPromise<any> {
        return new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((results: databaseStatisticsDto) => this.indexNames(results.Indexes.map(i => i.PublicName)));
    }

    fetchIndexDefinition(indexName: string) {
        new getIndexDefinitionCommand(indexName, this.activeDatabase())
            .execute()
            .done((dto: indexDefinitionContainerDto) => this.availableFields(dto.Index.Fields));
    }

    selectInitialIndex(indexToActivateOrNull: string) {
        if (indexToActivateOrNull && this.indexNames.contains(indexToActivateOrNull)) {
            this.setSelectedIndex(indexToActivateOrNull);
        } else if (this.indexNames().length > 0) {
            this.setSelectedIndex(this.indexNames.first());
        }
    }

    setSelectedIndex(indexName: string) {
        this.selectedIndexName(indexName);
        this.updateUrl(appUrl.forReporting(this.activeDatabase(), indexName));

        this.fetchIndexDefinition(indexName);
    }

    setSelectedField(fieldName: string) {
        this.selectedField(fieldName);
    }

    addValue(fieldName: string) {
        // Example queries:
        // /databases/TestDb/facets/Orders/ByCompany?&facetStart=0&facetPageSize=256&facets=%5B%7B%22Mode%22%3A0%2C%22Aggregation%22%3A1%2C%22AggregationField%22%3A%22Company%22%2C%22Name%22%3A%22Company%22%2C%22DisplayName%22%3A%22Company-Company%22%2C%22Ranges%22%3A%5B%5D%2C%22MaxResults%22%3Anull%2C%22TermSortMode%22%3A0%2C%22IncludeRemainingTerms%22%3Afalse%7D%5D&noCache=1402558754
        // /databases/TestDb/facets/Orders/ByCompany?&facetStart=0&facetPageSize=256&facets=[{"Mode":0,"Aggregation":1,"AggregationField":"Company","Name":"Company","DisplayName":"Company-Company","Ranges":[],"MaxResults":null,"TermSortMode":0,"IncludeRemainingTerms":false}]&noCache=1402558754
        var facet: facetDto = {
            Aggregation: 0,
            AggregationField: fieldName,
            DisplayName: fieldName + "-" + fieldName,
            IncludeRemainingTerms: false,
            MaxResults: null,
            Mode: 0,
            Name: fieldName,
            Ranges: [],
            TermSortMode: 0
        };

        this.addedValues.push(facet);
    }

    removeValue(facet: facetDto) {
        this.addedValues.remove(facet);
    }

    runReport() {
    }
}

export = reporting;