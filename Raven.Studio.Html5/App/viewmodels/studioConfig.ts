import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");
import document = require("models/document");

class studioConfig extends viewModelBase {

    systemDatabase: database;
    document = ko.observable<document>();
    systemDatabasePrompt = ko.observable<boolean>(false);

    constructor() {
        super();
        this.systemDatabase = appUrl.getSystemDatabase();
    }

    canActivate(args): any {
        var deffered = $.Deferred();

        new getDocumentWithMetadataCommand("Raven/StudioConfig", this.systemDatabase)
            .execute()
            .done((doc: document) => {
                this.document(doc);
                this.systemDatabasePrompt(doc["WarnWhenUsingSystemDatabase"]);
            })
            .always(() => deffered.resolve({ can: true }));

        return deffered;
    }

    compositionComplete() {
        super.compositionComplete();
        
    }

    saveStudioConfig() {
        //var saveCommand = new saveDocumentCommand(currentDocumentId, newDoc, this.systemDatabase);
        this.systemDatabasePrompt.toggle();
        require(["commands/saveDocumentCommand"], saveDocumentCommand => {

        });
    }
}

export = studioConfig;