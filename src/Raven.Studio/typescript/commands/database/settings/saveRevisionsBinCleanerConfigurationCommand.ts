import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import RevisionsBinConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsBinConfiguration;

class saveRevisionsBinCleanerConfigurationCommand extends commandBase {
  
  constructor(private db: database | string, private revisionsBinCleanerDto: RevisionsBinConfiguration) {
    super();
  }
  
  execute(): JQueryPromise<void> {
    const url = endpoints.databases.revisionsBinCleaner.adminRevisionsBinCleanerConfig;
    
    return this.post<void>(url, JSON.stringify(this.revisionsBinCleanerDto), this.db)
      .fail((response: JQueryXHR) => this.reportError("Failed to save Revisions bin cleaner configuration", response.responseText, response.statusText))
      .done(() => this.reportSuccess(`Revisions bin cleaner configuration saved successfully`))
  }
}

export = saveRevisionsBinCleanerConfigurationCommand;
