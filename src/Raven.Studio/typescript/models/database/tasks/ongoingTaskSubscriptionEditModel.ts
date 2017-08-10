/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskSubscriptionModel = require("models/database/tasks/ongoingTaskSubscriptionModel");
import getRevisionsConfigurationCommand = require("commands/database/documents/getRevisionsConfigurationCommand");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import collectionsTracker = require("common/helpers/database/collectionsTracker");

// This model is used by the 'Edit Subscription Task View'
class ongoingTaskSubscriptionEditModel extends ongoingTaskSubscriptionModel {

    collections = collectionsTracker.default.collections;

    script = ko.observable<string>();
    fromChangeVector = ko.observable<string>(null); 
    includeRevisions = ko.observable<boolean>(false);
    areRevisionsDefinedForCollection = ko.observable<boolean>(true);

    startingPointType = ko.observable<subscriptionStartType>();
    startingPointChangeVector: KnockoutComputed<boolean>;
    startingPointLatestDocument: KnockoutComputed<boolean>; 

    validationGroup: KnockoutValidationGroup; 

    activeDatabase = activeDatabaseTracker.default.database;
   
    constructor(dto: Raven.Client.Documents.Subscriptions.SubscriptionState) {
        super(dto);

        dto.Criteria.Script = dto.Criteria.Script || ""; 
        this.editViewUpdate(dto);
        this.editViewInitializeObservables(); 
        this.editViewInitValidation();
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

        this.collection.throttle(250).subscribe(() => { this.getCollectionRevisionsSettings(); }); 
        this.includeRevisions.throttle(250).subscribe(include => { if (include && this.collection()) { this.getCollectionRevisionsSettings(); } });
    }

    editViewUpdate(dto: Raven.Client.Documents.Subscriptions.SubscriptionState) {
        this.script(dto.Criteria.Script);
        this.fromChangeVector(dto.ChangeVector);
        this.includeRevisions(dto.Criteria.IncludeRevisions);
    }

    dataFromUI(): subscriptionDataFromUI {
        const script = _.trim(this.script()) || null;

        return {
            TaskName: this.taskName(),
            ChangeVectorEntry: null,
            // TODO:  Note: null means that we define with 'Beginning of Time'. This is temporary, until the other 2 options are implemented 
            Collection: this.collection(), 
            Script: script,
            IncludeRevisions: this.includeRevisions()
        }
    }

    editViewInitValidation() {

        this.collection.extend({
            required: true
        });
        
        this.script.extend({
            aceValidation: true
        });

        this.includeRevisions.extend({
            validation: [
                {
                    validator: () => this.collection(),
                    message: "Collection is not selected"
                },
                {
                    validator: () => !this.includeRevisions() || this.areRevisionsDefinedForCollection(),
                    message: "Revisions are not set for this collection"
                }]
        });

        this.validationGroup = ko.validatedObservable({
            collection: this.collection,
            includeRevisions: this.includeRevisions,
            script: this.script
        });
    }

    // Get the collections that have 'Revisons' set for them
    getCollectionRevisionsSettings() {
        return new getRevisionsConfigurationCommand(this.activeDatabase())
            .execute()
            .done((revisionsConfig: Raven.Client.ServerWide.Revisions.RevisionsConfiguration) => {
                if (revisionsConfig) {
                    let revisionIsSet: boolean = false;

                    // 1. Check for Default configuration
                    if (revisionsConfig.Default && revisionsConfig.Default.Active) {
                        revisionIsSet = true;
                    }

                    // 2. Check for specific collections configuration
                    for (var key in revisionsConfig.Collections) {
                        if (revisionsConfig.Collections.hasOwnProperty(key)) {
                            if (key === this.collection()) {
                                revisionIsSet = revisionsConfig.Collections[key].Active;
                                break;
                            }
                        }
                    };

                    this.areRevisionsDefinedForCollection(revisionIsSet);
                }
            });
    }

    createCollectionNameAutocompleter(item: ongoingTaskSubscriptionEditModel) {
        return ko.pureComputed(() => {
            const key = item.collection();

            const options = this.collections()
                .filter(x => !x.isAllDocuments && !x.isSystemDocuments && !x.name.startsWith("@"))
                .map(x => x.name);

            if (key) {
                return options.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return options;
            }
        });
    }

    static empty(): ongoingTaskSubscriptionEditModel {
        return new ongoingTaskSubscriptionEditModel(
            {
                Disabled: false,
                Criteria: {
                     Collection: null,
                     Script: "",
                     IncludeRevisions: false
                },
                ChangeVector: null,
                LastEtagReachedInServer: null,
                SubscriptionId: 0,
                SubscriptionName: null,
                TimeOfLastClientActivity: null
            });
    }
}

export = ongoingTaskSubscriptionEditModel;
