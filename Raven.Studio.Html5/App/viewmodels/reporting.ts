import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

class reporting extends viewModelBase {
    selectedIndexName = ko.observable<string>();
    indexNames = ko.observableArray<string>();
    hasSelectedIndex = ko.computed(() => this.selectedIndexName() && this.selectedIndexName().length > 0);
    editSelectedIndexUrl = ko.computed(() => this.hasSelectedIndex() ? appUrl.forEditIndex(this.selectedIndexName(), this.activeDatabase()) : null);

    setSelectedIndex(indexName: string) {
    }

    runReport() {
    }
}

export = reporting;