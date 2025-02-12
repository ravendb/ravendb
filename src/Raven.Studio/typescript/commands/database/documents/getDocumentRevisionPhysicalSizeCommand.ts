import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");


type DocumentRevisionPhysicalSizeCommandResult = Raven.Server.Documents.Handlers.SizeDetails & {
    ChangeVector: string;
  };

class getDocumentRevisionsPhysicalSizeCommand extends commandBase {
  
  constructor(private changeVectors: string, private db: database) {
    super();
  }
  
  execute(): JQueryPromise<DocumentRevisionPhysicalSizeCommandResult> {
    const args = {
      changeVector: this.changeVectors,
    };
    
    const url = endpoints.databases.revisions.revisionsSize + this.urlEncodeArgs(args);
    
    return this.query<DocumentRevisionPhysicalSizeCommandResult>(url, null, this.db)
      .fail((response: JQueryXHR) => this.reportError("Failed to get the revision physical size", response.responseText, response.statusText));
  }
}

export = getDocumentRevisionsPhysicalSizeCommand;
