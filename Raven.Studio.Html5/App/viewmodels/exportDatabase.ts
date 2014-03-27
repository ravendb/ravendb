import viewModelBase = require("viewmodels/viewModelBase");
import exportDatabaseCommand = require("commands/exportDatabaseCommand");

class exportDatabase extends viewModelBase {
  includeDocuments = ko.observable(true);
  includeIndexes = ko.observable(true);
  includeTransformers = ko.observable(true);
  includeAttachments = ko.observable(false);
  removeAnalyzers = ko.observable(false);

  startExport() {
    var smugglerOptions: smugglerOptionsDto = {
      IncludeDocuments: this.includeDocuments(),
      IncludeIndexes: this.includeIndexes(),
      IncludeTransformers: this.includeTransformers(),
      IncludeAttachments: this.includeAttachments(),
      RemoveAnalyzers: this.removeAnalyzers()
    };
    new exportDatabaseCommand(smugglerOptions, this.activeDatabase())
      .execute();
  }
}

export = exportDatabase;