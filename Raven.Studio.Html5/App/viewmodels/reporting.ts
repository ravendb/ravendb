import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");

class reporting extends viewModelBase {
    selectedIndexName = ko.observable<string>();
    selectedIndexLabel = ko.computed(() => this.selectedIndexName() ? this.selectedIndexName() : "[Select an index]");
    indexNames = ko.observableArray<string>();
    hasSelectedIndex = ko.computed(() => this.selectedIndexName() && this.selectedIndexName().length > 0);
    editSelectedIndexUrl = ko.computed(() => this.hasSelectedIndex() ? appUrl.forEditIndex(this.selectedIndexName(), this.activeDatabase()) : null);

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
    }

    runReport() {
    }
}

export = reporting;