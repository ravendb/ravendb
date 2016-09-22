import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import pagedResultSet = require("common/pagedResultSet");
import appUrl = require("common/appUrl");

class dataExplorationCommand extends commandBase {

    xhr: XMLHttpRequest;

    constructor(private request: dataExplorationRequestDto, private db: database) {
        super();
    }

    execute(): JQueryPromise<indexQueryResultsDto> {
        var self = this;
        var queryTask = this.query(this.getUrl(), null, this.db);
        queryTask.fail((response: JQueryXHR) => this.reportError("Error during query", response.responseText, response.statusText));
        return queryTask;
    }

    getUrl() {
        return "/streams/exploration/" + this.urlEncodeArgs(this.request);//TODO: use endpoints
    }

    getCsvUrl() {
        var requestWithCsvDownload: any = this.request;
        requestWithCsvDownload.download = "true";
        requestWithCsvDownload.format = "excel";
        return appUrl.forResourceQuery(this.db) + "/streams/exploration/" + this.urlEncodeArgs(requestWithCsvDownload);//TODO: use endpoints
    }
}

export = dataExplorationCommand;
