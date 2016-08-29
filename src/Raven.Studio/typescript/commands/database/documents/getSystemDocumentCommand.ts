import commandBase = require("commands/commandBase");

class getSystemDocumentCommand extends commandBase {

    constructor(private id: string) {
        super();
    }

    execute(): JQueryPromise<databaseDocumentDto> {

        var deferred = $.Deferred();

        var url = "/docs?id=" + this.id;//TODO: use endpoints
        var docQuery = this.query(url, null, null);
        docQuery.done((dto: databaseDocumentDto) => deferred.resolve(dto));
        docQuery.fail(response => deferred.reject(response));

        return deferred;
    }
}

export = getSystemDocumentCommand;
