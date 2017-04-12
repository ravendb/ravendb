import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class dataExplorationCommand extends commandBase {

    xhr: XMLHttpRequest;

    constructor(private request: dataExplorationRequestDto, private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Queries.QueryResult<any>> { //TODO avoid using any? 
        return this.query(this.getUrl(), null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Error during query", response.responseText, response.statusText));
    }

    getUrl() {
        return "/streams/exploration/" + this.urlEncodeArgs(this.request);//TODO: use endpoints
    }

    getCsvUrl() {
        var requestWithCsvDownload: any = this.request;
        requestWithCsvDownload.download = "true";
        requestWithCsvDownload.format = "excel";
        return appUrl.forDatabaseQuery(this.db) + "/streams/exploration/" + this.urlEncodeArgs(requestWithCsvDownload);//TODO: use endpoints
    }
}

export = dataExplorationCommand;
