import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class subscriptionQueryDefinitionPreview extends dialogViewModelBase {
    
    taskName = ko.observable<string>();
    query = ko.observable<string>();
    
    spinners = {
        loading: ko.observable<boolean>(true)
    };
    
    constructor(task: JQueryPromise<Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails>) {
        super();

        aceEditorBindingHandler.install();
        
        task.done(result => {
            this.query(result.Query);
            this.taskName(result.SubscriptionName);
        })
        .always(() => {
            this.spinners.loading(false);
        });
    }
}

export = subscriptionQueryDefinitionPreview;
