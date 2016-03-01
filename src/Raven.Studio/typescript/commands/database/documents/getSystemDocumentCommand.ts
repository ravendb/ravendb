import commandBase = require("commands/commandBase");
import pagedResultSet = require("common/pagedResultSet");

class getSystemDocumentCommand extends commandBase {

    constructor(private id: string) {
        super();
    }

    execute(): JQueryPromise<databaseDocumentDto> {

        var deferred = $.Deferred();

        var url = "/document?id=" + this.id;
        var docQuery = this.query(url, null, null);
        docQuery.done((dto: databaseDocumentDto) => deferred.resolve(dto));
        docQuery.fail(response => deferred.reject(response));

        return deferred;
    }
}

export = getSystemDocumentCommand;
