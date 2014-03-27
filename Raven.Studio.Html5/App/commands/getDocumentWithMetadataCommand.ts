import commandBase = require("commands/commandBase");
import database = require("models/database");
import document = require("models/document");

class getDocumentWithMetadataCommand extends commandBase {

    shouldResolveNotFoundAsNull: boolean;

    constructor(private id: string, private db: database, shouldResolveNotFoundAsNull?: boolean) {
        super();

        if (!id) {
            throw new Error("Must specify ID");
        }

        if (!db) {
            throw new Error("Must specify database");
        }
        this.shouldResolveNotFoundAsNull = shouldResolveNotFoundAsNull || false;
    }

    execute(): JQueryPromise<any> {

        // Executing /queries will return the doc with the metadata. 
        // We can do a GET call to /docs/<id>, however, it returns the metadata only as headers, 
        // which can have some issues when querying via CORS.
        var documentResult = $.Deferred();
        var postResult = this.post("/queries", JSON.stringify([this.id]), this.db);
        postResult.fail(xhr => documentResult.fail(xhr));
        postResult.done((queryResult: queryResultDto) => {
            if (queryResult.Results.length === 0) {
                if (this.shouldResolveNotFoundAsNull) {
                    documentResult.resolve(null);
                } else {
                    documentResult.reject("Unable to find document with ID " + this.id);
                }
            } else {
                documentResult.resolve(new document(queryResult.Results[0]));
            }
        });

        return documentResult;
    }

    // COMMENTED OUT:
    // Can't fetch doc by simply doing /docs/foo/123 because it has problems 
    // when running cross domain (e.g. during testing, or when connecting to remote servers).
    // In particular, we're unable to read custom response headers (e.g. ETag, __document_id, etc.)
    // for security reasons. See this: http://stackoverflow.com/questions/17581094/jquery-cors-and-custom-response-headers
    //
    // To fix this, we would need to set Access-Control-Expose-Headers on the response. (Add it to the server's Raven config file.)
    //
    // But for now, we're fetching docs by /queries, POSTING an array with a single doc ID in it.
    // This works because it returns the document metadata not as custom headers, but as a '@metadata' field in the results.

    //execute(): JQueryPromise<document> {
    //    var documentResult = $.Deferred();
    //    var url = "/docs/" + encodeURIComponent(this.id);
    //    var queryResult = this.query(url, null, this.db, null, true);
    //    queryResult.fail(xhr => documentResult.fail(xhr));
    //    queryResult.done((result: documentDto, status: any, xhr: JQueryXHR) => {
    //        // The metadata for the document is contained in the return headers.
    //        var nonAuthoritativeInfoHeader = xhr.getResponseHeader("Non-Authoritative-Information");
    //        var entityName = xhr.getResponseHeader("Raven-Entity-Name");
    //        var tempIndexScore = xhr.getResponseHeader("Temp-Index-Score");
    //        var ravenClrType = xhr.getResponseHeader("Raven-Clr-Type");
    //        result["@metadata"] = {
    //            "Raven-Entity-Name": entityName ? entityName : undefined,
    //            "Raven-Clr-Type": ravenClrType ? ravenClrType : undefined,
    //            "@id": xhr.getResponseHeader("__document_id"),
    //            "Last-Modified": xhr.getResponseHeader("Last-Modified"),
    //            "Raven-Last-Modified": xhr.getResponseHeader("Raven-Last-Modified"),
    //            "@etag": xhr.getResponseHeader("ETag"),
    //            "Non-Authoritative-Information": nonAuthoritativeInfoHeader === "True",
    //            "Temp-Index-Score": tempIndexScore ? parseInt(tempIndexScore, 10) : undefined
    //        };

    //        var document = new document(result);
    //        documentResult.resolve(document);
    //    });

    //    return documentResult;
    //}
 }

 export = getDocumentWithMetadataCommand;