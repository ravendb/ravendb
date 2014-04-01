import ace = require("ace/ace");
import router = require("plugins/router"); 

import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import document = require("models/document");
import database = require("models/database");

class databaseSettings extends viewModelBase {
   
    documentText = ko.observable('');
    docEditor: AceAjax.Editor;
    static containerId ="#databaseSettingsContainer";

    attached() {
        this.initializeDbDocEditor();
        this.fetchDatabaseSettings();
        this.createKeyboardShortcut("F2", () => this.navigateToDatabaseSettingDocument(), databaseSettings.containerId);
    }

    private initializeDbDocEditor() {
        // Startup the read-only Ace editor with JSON syntax highlighting.
        this.docEditor = ace.edit("dbDocEditor");
        this.docEditor.setTheme("ace/theme/github");
        this.docEditor.setFontSize("16px");
        this.docEditor.getSession().setMode("ace/mode/json");
        this.docEditor.setReadOnly(true);
    }

    private fetchDatabaseSettings() {
        var db = this.activeDatabase();
        if (db) {
            new getDatabaseSettingsCommand(db)
                .execute()
                .done(document => this.handleDocument(document));
        }
    }

    private getDatabaseSettingsDocumentId(db: database) {
        return "Raven/Databases/" + db.name;
    }

    private handleDocument(doc: document) {
        var dbSettings = this.stringify(doc.toDto());
        this.docEditor.getSession().setValue(dbSettings);
    }

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