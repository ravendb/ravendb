import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");
import document = require("models/document");

class studioConfig extends viewModelBase {

    systemDatabase: database;
    configDocument = ko.observable<document>();
    warnWhenUsingSystemDatabase = ko.observable<boolean>(true);
    remindToUpgrade = ko.observable<boolean>(false);
    
    private documentId = "Raven/StudioConfig";

    constructor() {
        super();
        this.systemDatabase = appUrl.getSystemDatabase();
    }

    canActivate(args): any {
        var deffered = $.Deferred();

        new getDocumentWithMetadataCommand(this.documentId, this.systemDatabase)
            .execute()
            .done((doc: document) => {
                this.configDocument(doc);
                this.warnWhenUsingSystemDatabase(doc["WarnWhenUsingSystemDatabase"]);
                this.remindToUpgrade(localStorage.getObject("LastServerBuildCheck") != null);
            })
            .fail(() => this.configDocument(document.empty()))
            .always(() => deffered.resolve({ can: true }));

        return deffered;
    }

    warnWhenUsingSystemDatabaseToggle() {
        var newDocument = this.configDocument();
        var action = !this.warnWhenUsingSystemDatabase();
        newDocument["WarnWhenUsingSystemDatabase"] = action;
        var saveTask = this.saveStudioConfig(newDocument);
        saveTask.done(() => this.warnWhenUsingSystemDatabase(action));
    }

    remingToUpgradeToggle() {
        var action = !this.remindToUpgrade();
        if (action) {
            localStorage.setObject("LastServerBuildCheck", new Date());
        } else {
            localStorage.removeItem("LastServerBuildCheck");
        }
        this.remindToUpgrade(action);
    }

    saveStudioConfig(newDocument: document) {
        var deferred = $.Deferred();

        require(["commands/saveDocumentCommand"], saveDocumentCommand => {
            var saveTask = new saveDocumentCommand(this.documentId, newDocument, this.systemDatabase).execute();
            saveTask
                .done((saveResult: bulkDocumentDto[]) => {
                    this.configDocument(newDocument);
                    this.configDocument().__metadata['@etag'] = saveResult[0].Etag;
                    deferred.resolve();
                })
                .fail(() => deferred.reject());
        });

        return deferred;
    }
}

export = studioConfig;