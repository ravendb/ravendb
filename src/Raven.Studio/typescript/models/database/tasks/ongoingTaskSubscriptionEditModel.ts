/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskSubscriptionModel = require("models/database/tasks/ongoingTaskSubscriptionModel");
import getRevisionsConfigurationCommand = require("commands/database/documents/getRevisionsConfigurationCommand");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import collectionsTracker = require("common/helpers/database/collectionsTracker");

// This model is used by the 'Edit Subscription Task View'
class ongoingTaskSubscriptionEditModel extends ongoingTaskSubscriptionModel {

    collections = collectionsTracker.default.collections;

    script = ko.observable<string>();
    fromChangeVector = ko.observableArray<Raven.Client.Documents.Replication.Messages.ChangeVectorEntry>([]); 
    includeRevisions = ko.observable<boolean>(true);
    areRevisionsDefinedForCollection = ko.observable<boolean>(true);

    startingPointType = ko.observable<subscriptionStartType>();
    startingPointChangeVector: KnockoutComputed<boolean>;
    startingPointLatestDocument: KnockoutComputed<boolean>; 

    validationGroup: KnockoutValidationGroup; 

    activeDatabase = activeDatabaseTracker.default.database;
   
    constructor(dto: Raven.Client.Documents.Subscriptions.SubscriptionState) {
        super(dto);

        this.editViewUpdate(dto);
        this.editViewInitializeObservables(); 
        this.editViewInitValidation();

        if (this.collection()) {
            this.getCollectionRevisionsSettings();
        }
    }

    editViewInitializeObservables() {
        super.listViewInitializeObservables();
        
        this.startingPointType("Beginning of Time");

        this.startingPointChangeVector = ko.pureComputed(() => {
            return this.startingPointType() === "Change Vector";
        });

        this.startingPointLatestDocument = ko.pureComputed(() => {
            return this.startingPointType() === "Latest Document";
        });

        this.collection.subscribe(() => { this.getCollectionRevisionsSettings(); });
        this.includeRevisions.subscribe(() => { if (this.includeRevisions()) { this.getCollectionRevisionsSettings(); } });
    }

    editViewUpdate(dto: Raven.Client.Documents.Subscriptions.SubscriptionState) {
        this.script(dto.Criteria.Script);
        this.fromChangeVector(dto.ChangeVector);
        this.includeRevisions(dto.Criteria.IsVersioned);
    }

    dataFromUI(): subscriptionDataFromUI {
        return {
            TaskName: this.taskName(),
            ChangeVectorEntry: null,
            // TODO:  Note: null means that we define with 'Beginning of Time'. This is temporary, until the other 2 options are implemented 
            Collection: this.collection(), 
            Script: this.script(),
            IsVersioned: this.includeRevisions()
        }
    }

    editViewInitValidation() {
        this.collection.extend({
            required: true,
            validation: [
                {
                    validator: (val: string) => { return _.find(this.collections(), (x) => { return x.name === val; }); },
                    message: "Collection doesn't exist"
                }
            ]
        });

        this.includeRevisions.extend({
            validation: [
                {
                    validator: (val: boolean) => !(this.includeRevisions() && !this.areRevisionsDefinedForCollection()),
                    message: "Revisions are not set for this collection"
                }]
        });

        this.validationGroup = ko.validatedObservable({
            collection: this.collection,
            includeRevisions: this.includeRevisions
        });
    }

    // Get the collections that have 'Revisons' set for them
    private getCollectionRevisionsSettings() {
        let revisionIsSet: boolean = false;

        let deferred = $.Deferred();
        new getRevisionsConfigurationCommand(this.activeDatabase())
            .execute()
            .done((revisionsConfig: Raven.Client.Server.Versioning.VersioningConfiguration) => {
                if (revisionsConfig) {

                    // 1. Check for Default configuration
                    if (revisionsConfig.Default && revisionsConfig.Default.Active) {
                        revisionIsSet = true;
                    } else {
                        // 2. Check for specific collections configuration
                        for (var key in revisionsConfig.Collections) {
                            if (revisionsConfig.Collections.hasOwnProperty(key)) {
                                if (key === this.collection() && revisionsConfig.Collections[key].Active) {
                                    revisionIsSet = true;
                                }
                            }
                        };
                    }
                }
            })
           .always(() => {
               deferred.resolve();
               this.areRevisionsDefinedForCollection(revisionIsSet);
            });

        return deferred;
    }

    static empty(): ongoingTaskSubscriptionEditModel {
        return new ongoingTaskSubscriptionEditModel(
            {
                Disabled: false,
                Criteria: {
                     Collection: null,
                     Script: null,
                     IsVersioned: false
                },
                ChangeVector: [],
                LastEtagReachedInServer: null,
                SubscriptionId: 0,
                SubscriptionName: null,
                TimeOfLastClientActivity: null
            });
    }
}

export = ongoingTaskSubscriptionEditModel;
