import viewModelBase = require("viewmodels/viewModelBase");
import getSubscriptionsCommand = require("commands/database/subscriptions/getSubscriptionsCommand");
import generalUtils = require("common/generalUtils");

class subscriptions extends viewModelBase {

    private subscriptions = ko.observableArray<subscriptionResponseItemDto>();

    static formatMillis = generalUtils.formatMillis;

    activate(args: any): JQueryPromise<queryResultDto<subscriptionResponseItemDto>> {
        super.activate(args);

        return this.fetchSubscriptions()
            .done(result => {
                this.subscriptions(result.Results);
            });
    }

    private fetchSubscriptions() {
        return new getSubscriptionsCommand(this.activeDatabase())
            .execute();
    }

}

export = subscriptions;
