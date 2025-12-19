import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import RemoteAttachmentsConfiguration = Raven.Client.Documents.Attachments.RemoteAttachmentsConfiguration;

class getRemoteAttachmentsConfigurationCommand extends commandBase {
  constructor(private db: database | string) {
    super();
  }

  execute(): JQueryPromise<RemoteAttachmentsConfiguration> {
    const url = endpoints.databases.remoteAttachment.adminAttachmentsRemoteConfig;

    const deferred = $.Deferred<RemoteAttachmentsConfiguration>();
    this.query<RemoteAttachmentsConfiguration>(url, null, this.db)
      .done((retiredAttachmentsConfig: RemoteAttachmentsConfiguration) => deferred.resolve(retiredAttachmentsConfig))
      .fail((response: JQueryXHR) => {
        if (response.status === 404) {
          deferred.resolve(null);
        } else {
          deferred.reject(response);
          this.reportError("Failed to get remote attachments configuration", response.responseText, response.statusText);
        }
      });

    return deferred;
  }
}

export = getRemoteAttachmentsConfigurationCommand;
