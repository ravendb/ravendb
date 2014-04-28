import ace = require("ace/ace");
import router = require("plugins/router"); 

import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import document = require("models/document");
import database = require("models/database");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");

class databaseSettings extends viewModelBase {
   
    document = ko.observable<document>();
    documentText = ko.observable<string>().extend({ required: true });
    docEditor: AceAjax.Editor;
    isSaveEnabled: KnockoutComputed<Boolean>;
    static containerId ="#databaseSettingsContainer";

    constructor() {
        super();
        aceEditorBindingHandler.install();
        this.document.subscribe(doc => {
            if (doc) {
                var docText = this.stringify(doc.toDto());
                this.documentText(docText);
            }
        });
    }

    canActivate(args) {
        super.canActivate(args);

        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            new getDatabaseSettingsCommand(db)
                .execute()
                .done(document => {
                    this.document(document);
                    deferred.resolve({ can: true });
                })
                .fail(() => deferred.resolve({ redirect: appUrl.forSettings(db) }));
        }

        return deferred;
    }

    attached() {
        this.initializeDbDocEditor();
        //this.docEditor.getSession().setValue(this.documentText());
        //this.fetchDatabaseSettings();
        //this.createKeyboardShortcut("F2", () => this.navigateToDatabaseSettingDocument(), databaseSettings.containerId);
    }

    compositionComplete() {
        super.compositionComplete();
        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.documentText]);
        var self = this;
        this.isSaveEnabled = ko.computed(() => {
            return viewModelBase.dirtyFlag().isDirty();
        });
        this.initializeDbDocEditor();
    }

    private initializeDbDocEditor() {
        // Startup the read-only Ace editor with JSON syntax highlighting.
        this.docEditor = ace.edit("dbDocEditor");
        this.docEditor.setTheme("ace/theme/github");
        this.docEditor.setFontSize("16px");
        this.docEditor.getSession().setMode("ace/mode/json");
        this.docEditor.setReadOnly(true);
    }

    private getDatabaseSettingsDocumentId(db: database) {
        return "Raven/Databases/" + db.name;
    }

    //private handleDocument(doc: document) {
    //    var dbSettings = this.stringify(doc.toDto());
    //    this.docEditor.getSession().setValue(dbSettings);
    //}

    private stringify(obj: any) {
        var prettifySpacing = 4;
        return JSON.stringify(obj, null, prettifySpacing);
    }

    navigateToDatabaseSettingDocument() {
        var db = this.activeDatabase();
        if (db) {
            var documentId = this.getDatabaseSettingsDocumentId(db);
            var systemDatabase = appUrl.getSystemDatabase();
            var dbSettingsUrl = appUrl.forEditDoc(documentId, null, null, systemDatabase);
            router.navigate(dbSettingsUrl);
        }
    }
}

export = databaseSettings;