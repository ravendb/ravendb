import commandBase = require("commands/commandBase");
import database = require("models/resources/database");


class setSubscriptionAckEtagCommand extends commandBase {

    constructor(private db: database, private id: number, private newEtag: string) {
        super();

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): any {
        var args = {
            id: this.id,
            newEtag: this.newEtag
        };
        var url = "/subscriptions/setSubscriptionAckEtag" + this.urlEncodeArgs(args);
        return this.post(url, null, this.db, { dataType: undefined })
            .done(() => this.reportSuccess("Successfully set acknowledged etag"))
            .fail((response) => this.reportError("Failed to set subscription acknowledged etag!", response.responseText, response.statusText));
    }
}

export = setSubscriptionAckEtagCommand;
