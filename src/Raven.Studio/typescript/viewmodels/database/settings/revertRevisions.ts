import viewModelBase = require("viewmodels/viewModelBase");
import datePickerBindingHandler = require("common/bindingHelpers/datePickerBindingHandler");
import revertRevisionsCommand = require("commands/database/documents/revertRevisionsCommand");
import revertRevisionsRequest = require("models/database/documents/revertRevisionsRequest");
import notificationCenter = require("common/notifications/notificationCenter");
import appUrl = require("common/appUrl");
import moment = require("moment");
import collectionsTracker from "common/helpers/database/collectionsTracker";

class revertRevisions extends viewModelBase {
    
    view = require("views/database/settings/revertRevisions.html");

    model = new revertRevisionsRequest();
    revisionsUrl: KnockoutComputed<string>;

    allExistingCollections: KnockoutComputed<string[]>;
    collectionToAdd = ko.observable<string>();
    canAddAllCollections: KnockoutComputed<boolean>;
    
    datePickerOptions = {
        format: revertRevisionsRequest.defaultDateFormat,
        maxDate: moment.utc().add(10, "minutes").toDate() // add 10 minutes to avoid issues with time skew
    };
    
    spinners = {
        revert: ko.observable<boolean>(false)
    };
    
    static magnitudes: timeMagnitude[] = ["minutes", "hours", "days"];

    constructor() {
        super();

        this.initObservables();
        
        this.bindToCurrentInstance("setMagnitude", "createCollectionNameAutocompleter", "addWithBlink", "removeCollection");
        datePickerBindingHandler.install();
    }
    
    private initObservables() {
        this.revisionsUrl = ko.pureComputed(() => {
            return appUrl.forRevisions(this.activeDatabase());
        });
        this.allExistingCollections = ko.pureComputed(() => collectionsTracker.default.getCollectionNames().filter(x => x !== "@empty" && x !== "@hilo"));

        this.canAddAllCollections = ko.pureComputed(() => {
            return _.difference(this.allExistingCollections(), this.model.collectionsToRevert()).length > 0;
        });
    }
    
    setMagnitude(value: timeMagnitude) {
        this.model.windowMagnitude(value);
    }
    
    run() {
        if (this.isValid(this.model.validationGroup)) {
            const db = this.activeDatabase();
            
            this.confirmationMessage("Revert Revisions", "Do you want to revert documents state to date: " + this.model.pointInTimeFormatted() + " UTC?", {
                buttons: ["No", "Yes, revert"]
                })
                .done(result => {
                    if (result.can) {
                        this.spinners.revert(true);
                        
                        const dto = this.model.toDto();
                        new revertRevisionsCommand(dto, db)
                            .execute()
                            .done((operationIdDto: operationIdDto) => {
                                const operationId = operationIdDto.OperationId;
                                notificationCenter.instance.openDetailsForOperationById(db, operationId);
                            })
                            .always(() => this.spinners.revert(false));
                    }
                })
        }
    }

    createCollectionNameAutocompleter() {
        return ko.pureComputed(() => {
            const key = this.collectionToAdd();

            const options = this.allExistingCollections();
            const usedOptions = this.model.collectionsToRevert();
            const filteredOptions = _.difference(options, usedOptions);

            if (key) {
                return filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return filteredOptions;
            }
        });
    }

    addCollection() {
        this.addWithBlink(this.collectionToAdd());
    }

    addWithBlink(collectionName: string) {
        if (!this.model.collectionsToRevert().find(x => x === collectionName)) {
            this.model.collectionsToRevert.unshift(collectionName);
        }

        this.collectionToAdd("");

        $(".collection-list li").first().addClass("blink-style");
    }

    addAllCollections() {
        const collections = _.uniq(this.model.collectionsToRevert().concat(this.allExistingCollections())).sort();
        this.model.collectionsToRevert(collections);
    }

    removeCollection(collectionName: string) {
        this.model.collectionsToRevert.remove(collectionName);
    }
}

export = revertRevisions;
