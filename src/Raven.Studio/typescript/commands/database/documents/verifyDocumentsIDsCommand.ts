import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class verifyDocumentsIDsCommand extends commandBase {

    static IDsLocalStorage: Array<string> = [];
    static InvalidIDsLocal: Array<string> = [];

    constructor(private docIDs: string[], private db: database, private queryLocalStorage:boolean, private storeResultsInLocalStorage:boolean) {
        super();

        if (!docIDs) {
            throw new Error("Must specify IDs");
        }

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<Array<string>> {
        
        var verifyResult = $.Deferred<Array<string>>();       
        var verifiedIDs: string[] = [];

        // if required to check with locally stored document ids first, remove known non existing documet ids first and confirm verified ids later
        if (this.queryLocalStorage) {

            if (verifyDocumentsIDsCommand.InvalidIDsLocal.length > 0) {
                this.docIDs.removeAll(verifyDocumentsIDsCommand.InvalidIDsLocal);
            }

            if (verifyDocumentsIDsCommand.IDsLocalStorage.length > 0) {
                this.docIDs.forEach(curId => {
                    if (verifyDocumentsIDsCommand.IDsLocalStorage.first(x => x === curId)) {
                        verifiedIDs.push(curId);
                    } 
                });

                this.docIDs.removeAll(verifyDocumentsIDsCommand.IDsLocalStorage);
            }
        } 

        if (this.docIDs.length > 0) {
            var postResult = this.post("/docs?metadata-only=true", JSON.stringify(this.docIDs), this.db);
            postResult.fail((xhr: JQueryXHR) => verifyResult.reject(xhr));
            postResult.done((queryResult: queryResultDto) => {
                if (queryResult && queryResult.Results) {
                    queryResult.Results.forEach(curVerifiedID => {
                        verifiedIDs.push(curVerifiedID['@metadata']['@id']);                        
                        if (this.queryLocalStorage) {
                            verifyDocumentsIDsCommand.IDsLocalStorage.push(curVerifiedID);
                        }
                    });

                    if (this.queryLocalStorage) {
                        this.docIDs.removeAll(queryResult.Results.map(curResult => curResult['@metadata']['@id']));
                        verifyDocumentsIDsCommand.InvalidIDsLocal.pushAll(this.docIDs);
                    }
                }
                verifyResult.resolve(verifiedIDs);
            });
            return verifyResult;
        } else {
            verifyResult.resolve(verifiedIDs);
            return verifyResult;
        }
    }
}

export = verifyDocumentsIDsCommand;
