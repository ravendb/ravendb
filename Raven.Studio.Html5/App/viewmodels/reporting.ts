import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getIndexDefinitionCommand = require("commands/getIndexDefinitionCommand");
import facet = require("models/facet");
import queryFacetsCommand = require("commands/queryFacetsCommand");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");

class reporting extends viewModelBase {
    selectedIndexName = ko.observable<string>();
    selectedIndexLabel = ko.computed(() => this.selectedIndexName() ? this.selectedIndexName() : "[Select an index]");
    indexNames = ko.observableArray<string>();
    hasSelectedIndex = ko.computed(() => this.selectedIndexName() && this.selectedIndexName().length > 0);
    editSelectedIndexUrl = ko.computed(() => this.hasSelectedIndex() ? appUrl.forEditIndex(this.selectedIndexName(), this.activeDatabase()) : null);
    availableFields = ko.observableArray<string>();
    selectedField = ko.observable<string>();
    selectedFieldLabel = ko.computed(() => this.selectedField() ? this.selectedField() : "Select a field");
    addedValues = ko.observableArray<facet>();
    filter = ko.observable<string>();
    hasFilter = ko.observable(false);
    reportResults = ko.observable<pagedList>();

    activate(indexToActivateOrNull: string) {
        super.activate(indexToActivateOrNull);

        this.fetchIndexes()
            .done(() => this.selectInitialIndex(indexToActivateOrNull));

        aceEditorBindingHandler.install();
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

        // Update all facets to use that too.
        this.addedValues().forEach(v => v.name = fieldName);
    }

    addValue(fieldName: string) {
        var val = facet.fromNameAndAggregation(this.selectedField(), fieldName);
        this.addedValues.push(val);
    }

    removeValue(val: facet) {
        this.addedValues.remove(val);
    }

    runReport() {
        var selectedIndex = this.selectedIndexName();
        var filterQuery = this.hasFilter() ? this.filter() : null;
        var facets = this.addedValues().map(v => v.toDto());
        var db = this.activeDatabase();
        var resultsFetcher = (skip: number, take: number) => new queryFacetsCommand(selectedIndex, filterQuery, skip, take, facets, db).execute();
        this.reportResults(new pagedList(resultsFetcher));
    }
}

export = reporting;