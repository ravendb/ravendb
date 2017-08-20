/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskSubscriptionModel = require("models/database/tasks/ongoingTaskSubscriptionModel");
import getRevisionsConfigurationCommand = require("commands/database/documents/getRevisionsConfigurationCommand");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import collectionsTracker = require("common/helpers/database/collectionsTracker");

// This model is used by the 'Edit Subscription Task View'
class ongoingTaskSubscriptionEditModel extends ongoingTaskSubscriptionModel {

    collections = collectionsTracker.default.collections;
    liveConnection = ko.observable<boolean>();

    includeRevisions = ko.observable<boolean>(false);
    areRevisionsDefinedForCollection = ko.observable<boolean>(true);

    script = ko.observable<string>();

    startingPointType = ko.observable<subscriptionStartType>();
    startingChangeVector = ko.observable<string>();
    startingPointChangeVector: KnockoutComputed<boolean>;
    startingPointLatestDocument: KnockoutComputed<boolean>; 
    setStartingPoint = ko.observable<boolean>(true);
    
    changeVectorForNextBatchStartingPoint = ko.observable<string>(null); 

    validationGroup: KnockoutValidationGroup; 

    activeDatabase = activeDatabaseTracker.default.database;
   
    constructor(dto: Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails, isEdit: boolean) {        super(dto);

        this.isEdit = isEdit;
        dto.Criteria.Script = dto.Criteria.Script || ""; 
        this.editViewUpdate(dto);
        this.editViewInitializeObservables(); 
        this.editViewInitValidation();
    }

    editViewInitializeObservables() {
        super.listViewInitializeObservables();
        
        this.startingPointType("Beginning of Time");

        this.startingPointChangeVector = ko.pureComputed(() => {
            return this.startingPointType() === "Change-Vector";
        });

        this.startingPointLatestDocument = ko.pureComputed(() => {
            return this.startingPointType() === "Latest Document";
        });

        this.collection.throttle(250).subscribe(() => { this.getCollectionRevisionsSettings(); }); 
        this.includeRevisions.throttle(250).subscribe(include => { if (include && this.collection()) { this.getCollectionRevisionsSettings(); } });
    }

    editViewUpdate(dto: Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails) {
        this.script(dto.Criteria.Script);
        this.includeRevisions(dto.Criteria.IncludeRevisions);
        this.changeVectorForNextBatchStartingPoint(dto.ChangeVectorForNextBatchStartingPoint);
        this.setStartingPoint(false);
    }

    dataFromUI(): subscriptionDataFromUI {
        const script = _.trim(this.script()) || null;
        
        let changeVector: Raven.Client.Constants.Documents.SubscriptionChangeVectorSpecialStates | string = "DoNotChange";

        if (this.setStartingPoint()) {
            switch (this.startingPointType()) {
            case "Beginning of Time":
                {
                    changeVector = "BeginningOfTime";
                };
                break;
            case "Latest Document":
                {
                    changeVector = "LastDocument";
                };
                break;
            case "Change-Vector":
                {
                    changeVector = this.startingChangeVector();
                };
                break;
            }
        }

        return {
            TaskName: this.taskName(),
            Collection: this.collection(),
            Script: script,
            IncludeRevisions: this.includeRevisions(),
            ChangeVector: changeVector
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

        this.startingChangeVector.extend({
            validation: [
                {
                    validator: () => {
                        const goodState1 = this.setStartingPoint() && this.startingPointType() === 'Change-Vector' && this.startingChangeVector();
                        const goodState2 = this.setStartingPoint() && this.startingPointType() !== 'Change-Vector';
                        const goodState3 = !this.setStartingPoint();
                        return goodState1 || goodState2 || goodState3;
                    },
                    message: "Please enter change-vector"
                }]
        });

        this.validationGroup = ko.validatedObservable({
            collection: this.collection,
            includeRevisions: this.includeRevisions,
            script: this.script,
            startingChangeVector: this.startingChangeVector
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
                ChangeVectorForNextBatchStartingPoint: null,
                SubscriptionId: 0,
                SubscriptionName: null,
                ResponsibleNode: null,
                LastClientConnectionTime: null,
                LastTimeServerMadeProgressWithDocuments: null
            }, true);
    }
}

export = ongoingTaskSubscriptionEditModel;
