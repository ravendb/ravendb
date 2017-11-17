import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getSubscriptionsCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<queryResultDto<subscriptionResponseItemDto>> {
        return this.query<queryResultDto<subscriptionResponseItemDto>>(endpoints.databases.subscriptions.subscriptions, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to load subscriptions", response.responseText));
    }
}

export = getSubscriptionsCommand;
