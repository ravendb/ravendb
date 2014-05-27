import commandBase = require("commands/commandBase");
import database = require("models/database");
import document = require("models/document");

class getDocumentsStartingWithCommand extends commandBase {

  constructor(private startingWith: string, private db: database, private start: number = 0, private pageSize = 100) {
    super();

    if (!startingWith) {
      throw new Error("Must specify ID");
    }

    if (!db) {
      throw new Error("Must specify database");
    }
  }

  execute(): JQueryPromise<any> {
    var documentResult = $.Deferred();
    var url ="/docs?startsWith=" + encodeURIComponent(this.startingWith) +
      "&start=" + encodeURIComponent(this.start.toString()) +
      "&pageSize=" + encodeURIComponent(this.pageSize.toString());
    var postResult = this.query(url, null, this.db);
    postResult.fail(xhr => documentResult.fail(xhr));
    postResult.done((queryResult: any[]) => {
      documentResult.resolve(queryResult.map(d => {
        return new document(d);
      }));
    });
    return documentResult;
  }

}

export = getDocumentsStartingWithCommand;