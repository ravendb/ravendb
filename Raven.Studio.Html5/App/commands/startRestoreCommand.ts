import deleteDocumentCommand = require("commands/deleteDocumentCommand");
import commandBase = require("commands/commandBase");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");

class startRestoreCommand extends commandBase {

  constructor(private db: database, private defrag: boolean, private restoreRequest: restoreRequestDto, private updateRestoreStatus: (restoreStatusDto)=> void) {
    super();
  }

  execute(): JQueryPromise<any> {
    var result = $.Deferred();

    new deleteDocumentCommand('Raven/Restore/Status', this.db)
      .execute()
      .fail(response=> result.reject(response))
      .done(_=> {
        this.post('/admin/restore?defrag=' + this.defrag, ko.toJSON(this.restoreRequest), this.db, { dataType: 'text' })
          .fail(response=> {
            var r = JSON.parse(response.responseText);
            var restoreStatus: restoreStatusDto = { Messages: [r.Error], IsRunning: false };
            this.updateRestoreStatus(restoreStatus);
            result.reject(response);
          })
          .done(response=> {
            this.getRestoreStatus(result);
          });
      });

    return result;
  }

  private getRestoreStatus(result: JQueryDeferred<any>) {

    new getDocumentWithMetadataCommand("Raven/Restore/Status", this.db)
      .execute()
      .fail(response=> result.reject(response))
      .done((restoreStatus: restoreStatusDto)=> {

        var lastMessage = restoreStatus.Messages.last();
        var isRestoreFinished =
          lastMessage.contains("The new database was created") ||
            lastMessage.contains("Restore Canceled") ||
            lastMessage.contains("A database name must be supplied if the restore location does not contain a valid Database.Document file") ||
            lastMessage.contains("Cannot do an online restore for the <system> database") ||
            lastMessage.contains("Restore ended but could not create the datebase document, in order to access the data create a database with the appropriate name");

        restoreStatus.IsRunning = !isRestoreFinished;
        this.updateRestoreStatus(restoreStatus);

        if (!isRestoreFinished) {
          setTimeout(()=> this.getRestoreStatus(result), 1000);
        }
      });

  }

}

export = startRestoreCommand;