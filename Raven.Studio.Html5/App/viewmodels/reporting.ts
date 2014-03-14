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
        //var facet: facetDto = {
        //    Aggregation 
        //};
    }

    runReport() {
    }
}

export = reporting;