import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import RemoteAttachmentsConfiguration = Raven.Client.Documents.Attachments.RemoteAttachmentsConfiguration;

class saveRemoteAttachmentsConfigurationCommand extends commandBase {
  constructor(private db: database | string, private remoteAttachmentsConfiguration: RemoteAttachmentsConfiguration) {
    super();
  }

  execute(): JQueryPromise<void> {
    const url = endpoints.databases.remoteAttachment.adminAttachmentsRemoteConfig;

    return this.put<void>(url, JSON.stringify(this.remoteAttachmentsConfiguration), this.db)
      .fail((response: JQueryXHR) => this.reportError("Failed to save remote attachments configuration", response.responseText, response.statusText))
      .done(() => this.reportSuccess(`Remote attachments configuration saved successfully`))
  }
}

export = saveRemoteAttachmentsConfigurationCommand;
