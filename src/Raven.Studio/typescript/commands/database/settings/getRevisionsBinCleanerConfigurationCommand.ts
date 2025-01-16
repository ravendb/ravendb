import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import RevisionsBinConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsBinConfiguration;

class getRevisionsBinCleanerConfigurationCommand extends commandBase {
  
  constructor(private db: database | string) {
    super();
  }
  
  execute(): JQueryPromise<RevisionsBinConfiguration> {
    const url = endpoints.databases.revisionsBinCleaner.revisionsBinCleanerConfig;
    
    const deferred = $.Deferred<RevisionsBinConfiguration>();
    this.query<RevisionsBinConfiguration>(url, null, this.db)
      .done((revisionBinCleanerConfig: RevisionsBinConfiguration) => deferred.resolve(revisionBinCleanerConfig))
      .fail((response: JQueryXHR) => {
        if (response.status === 404) {
          deferred.resolve(null);
        } else {
          deferred.reject(response);
          this.reportError("Failed to get Revisions bin cleaner configuration", response.responseText, response.statusText);
        }
      });
    
    return deferred;
  }
}

export = getRevisionsBinCleanerConfigurationCommand;
