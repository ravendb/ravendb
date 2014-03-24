import viewModelBase = require("viewmodels/viewModelBase");
import exportDatabaseCommand = require("commands/exportDatabaseCommand");

class exportDatabase extends viewModelBase {
  includeDocuments = ko.observable(true);
  includeIndexes = ko.observable(true);
  includeTransformers = ko.observable(true);
  includeAttachments = ko.observable(false);
  removeAnalyzers = ko.observable(false);

  startExport() {
    new exportDatabaseCommand(this.activeDatabase())
      .execute();
  }
}

export = exportDatabase;