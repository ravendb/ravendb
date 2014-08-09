import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");
import document = require("models/document");

class studioConfig extends viewModelBase {

    systemDatabase: database;
    systemDatabasePrompt = ko.observable<boolean>(true);
    configDocument = ko.observable<document>();
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
                this.systemDatabasePrompt(doc["WarnWhenUsingSystemDatabase"]);
            })
            .fail(() => this.configDocument(document.empty()))
            .always(() => deffered.resolve({ can: true }));

        return deffered;
    }

    saveStudioConfig() {
        var db = this.activeDatabase();
        if (db) {
            var doc = this.configDocument();
            var action = !this.systemDatabasePrompt();
            doc["WarnWhenUsingSystemDatabase"] = action;
            require(["commands/saveDocumentCommand"], saveDocumentCommand => {
                var saveTask = new saveDocumentCommand(this.documentId, doc, this.systemDatabase).execute();
                saveTask
                    .done((saveResult: bulkDocumentDto[]) => {
                        this.systemDatabasePrompt(action);
                        this.configDocument().__metadata['@etag'] = saveResult[0].Etag;
                    });
            });
        }
    }
}

export = studioConfig;