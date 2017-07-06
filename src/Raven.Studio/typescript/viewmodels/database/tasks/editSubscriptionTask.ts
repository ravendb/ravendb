import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import ongoingTaskSubscription = require("models/database/tasks/ongoingTaskSubscriptionModel");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import collection = require("models/database/documents/collection");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand"); 
import collectionsStats = require("models/database/documents/collectionsStats");
import saveSubscriptionTaskCommand = require("commands/database/tasks/saveSubscriptionTaskCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class editSubscriptionTask extends viewModelBase {

    editedSubscription = ko.observable<ongoingTaskSubscription>();
    isAddingNewSubscriptionTask = ko.observable<boolean>(true);
    collections = ko.observableArray<collection>([]);

    constructor() {
        super();
        this.bindToCurrentInstance("useCollection", "setStartingPointType");
        aceEditorBindingHandler.install();
    }

    activate(args: any) { 
        super.activate(args);

        if (args.taskId) { 

            // 1. Editing an existing task
            this.isAddingNewSubscriptionTask(false);

            new ongoingTaskInfoCommand(this.activeDatabase(), "Subscription", args.taskId, args.taskName)
                .execute()
                .done((result: Raven.Client.Documents.Subscriptions.SubscriptionState) => this.editedSubscription(new ongoingTaskSubscription(result))) 
                .fail(() => router.navigate(appUrl.forOngoingTasks(this.activeDatabase())));
        }
        else {
            // 2. Creating a new task
            this.isAddingNewSubscriptionTask(true);
            this.editedSubscription(ongoingTaskSubscription.empty());
        }

        return $.when<any>(this.fetchAllCollections());
    }

    compositionComplete() {
        super.compositionComplete();
        document.getElementById('taskName').focus(); 
        document.getElementById("toggle-transform-script").click();
    }

    saveSubscription() {
        //1. Validate model
        if (!this.validate()) { 
             return;
        }

        // 2. Create/add the new replication task
        const dtoDataFromUI = this.editedSubscription().dataFromUI();

        new saveSubscriptionTaskCommand(this.activeDatabase(), dtoDataFromUI, this.editedSubscription().taskId, this.editedSubscription().taskState()) 
            .execute()
            .done(() => this.goToOngoingTasksView());
    }

    cloneSubscription() {
        this.isAddingNewSubscriptionTask(true);
        this.editedSubscription().taskName("");
        this.editedSubscription().taskId = null;
        document.getElementById('taskName').focus(); 
    }
   
    cancelOperation() {
        this.goToOngoingTasksView();
    }

    useCollection(collectionToUse: collection) {
        this.editedSubscription().collection(collectionToUse.name);
    }

    setStartingPointType(startingPointType: string) {
        this.editedSubscription().startingPointType(startingPointType);
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    private validate(): boolean {
        let valid = true;

        if (!this.isValid(this.editedSubscription().validationGroup))
            valid = false;

        return valid;
    }

    private fetchAllCollections(): JQueryPromise<collectionsStats> { // TODO: duplicate code with patch.ts - can we merge ?
        return new getCollectionsStatsCommand(this.activeDatabase())
            .execute()
            .done((stats: collectionsStats) => {
                this.collections(stats.collections);
            });
    }
}

export = editSubscriptionTask;