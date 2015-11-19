import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import Subscription = require("models/database/subscription/subscription");

class getSubscriptionCommand extends commandBase {

    constructor(private db: database) {
        super();

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<Array<Subscription>> {
        var url = "/debug/subscriptions";

        var resultsSelector = (subscriptions: Array<subscriptionDto>) => subscriptions.map((subscription: subscriptionDto) =>
            new Subscription(subscription.SubscriptionId, subscription.AckEtag));
        var rc = this.query(url, null, this.db, resultsSelector);
        return rc;
    }
}

export = getSubscriptionCommand;
