import dialogViewModelBase from "viewmodels/dialogViewModelBase";
import database from "models/resources/database";
import enforceRevisionsConfigurationCommand from "commands/database/settings/enforceRevisionsConfigurationCommand";
import notificationCenter from "common/notifications/notificationCenter";
import dialog = require("plugins/dialog");
import collectionsTracker from "common/helpers/database/collectionsTracker";


class enforceRevisionsModel {
    includeForceCreated = ko.observable<boolean>(false);
    includeAllCollections = ko.observable<boolean>(true);
    includedCollections = ko.observableArray<string>([]);
    
    validationGroup: KnockoutValidationGroup;
    
    constructor() {
        this.includedCollections.extend({
            validation: [
                {
                    validator: () => this.includeAllCollections() || this.includedCollections().length > 0,
                    message: "At least one collection is required"
                }
            ]
        });
        
        this.validationGroup = ko.validatedObservable({
            includedCollections: this.includedCollections
        });
    }
}

class enforceRevisions extends dialogViewModelBase {
    view = require("views/database/settings/enforceRevisions.html");
    
    private readonly db: database;
    private readonly model: enforceRevisionsModel = new enforceRevisionsModel();
    
    collections: string[];
    
    constructor(db: database) {
        super();
        this.db = db;
        
        this.collections = collectionsTracker.default.getCollectionNames();
    }
    
    confirm() {
        if (!this.isValid(this.model.validationGroup)) {
            return;
        }
        
        new enforceRevisionsConfigurationCommand(this.db, this.model.includeForceCreated(), this.model.includeAllCollections() ? null : this.model.includedCollections())
            .execute()
            .done((operationIdDto: operationIdDto) => {
                const operationId = operationIdDto.OperationId;
                notificationCenter.instance.openDetailsForOperationById(this.db, operationId);
            });
        
        dialog.close(this, true);
    }

    cancel() {
        dialog.close(this, false);
    }
}

export = enforceRevisions;
