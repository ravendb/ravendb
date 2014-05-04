import ace = require("ace/ace");
import router = require("plugins/router"); 
import app = require("durandal/app");
import appUrl = require("common/appUrl");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import saveDatabaseSettingsCommand = require("commands/saveDatabaseSettingsCommand");
import document = require("models/document");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import viewSystemDatabaseConfirm = require("viewmodels/viewSystemDatabaseConfirm");

class databaseSettings extends viewModelBase {
   
    document = ko.observable<document>();
    documentText = ko.observable<string>().extend({ required: true });
    docEditor: AceAjax.Editor;
    securedSettings: string;
    updatedDto: documentDto;
    textarea: any;
    isSaveEnabled: KnockoutComputed<Boolean>;
    isEditingEnabled = ko.observable<boolean>(false);


    static containerId ="#databaseSettingsContainer";

    constructor() {
        super();
        aceEditorBindingHandler.install();
        this.document.subscribe(doc => {
            if (doc) {
                var docDto: any = doc.toDto();
                this.securedSettings = ko.toJSON(docDto.SecuredSettings);
                var docText = this.stringify(doc.toDto());
                this.documentText(docText);
            }
        });
    }

    canActivate(args) {
        super.canActivate(args);
        var deferred = $.Deferred();
        var db = this.activeDatabase();
        this.fetchDatabaseSettings(db)
            .done(()=> {
                deferred.resolve({ can: true });
            })
            .fail(() => deferred.resolve({ redirect: appUrl.forStatus(db) }));
        return deferred;
    }

    activate(args) {
        super.activate(args);

        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.documentText]);
        this.isSaveEnabled = ko.computed(() => {
            return viewModelBase.dirtyFlag().isDirty();
        });
    }

    compositionComplete() {
        super.compositionComplete();
        this.docEditor = ko.utils.domData.get($("#dbDocEditor")[0], "aceEditor");
        this.textarea = $(this.docEditor.container).find('textarea')[0];

        this.documentText.subscribe(docText=> {
            var message = "";
            try {
                this.updatedDto = JSON.parse(docText);
            }
            catch (e) {
                message = "This isn't a legal JSON expression!";
            }
            this.textarea.setCustomValidity(message);
        });
    }

    private fetchDatabaseSettings(db: database, reportFetchProgress: boolean = false): JQueryPromise<any> {
        return new getDatabaseSettingsCommand(db, reportFetchProgress)
            .execute()
            .done((document: document)=> { this.document(document); });
    }

    formatDocument() {
        try {
            var tempDoc = JSON.parse(this.documentText());
            var formatted = this.stringify(tempDoc);
            this.documentText(formatted);
        }
        catch (e){}
    }

    editDatabaseSettings() {
        var editDbConfirm = new viewSystemDatabaseConfirm("Meddling with the database settings document could cause irreversible damage!");
        editDbConfirm
            .viewTask
            .done(() => {
                this.docEditor.setReadOnly(false);
                this.docEditor.focus();
                this.isEditingEnabled(true);
            });
        app.showDialog(editDbConfirm);
    }

    saveChanges() {
        var editDbConfirm = new viewSystemDatabaseConfirm("Meddling with the database settings document could cause irreversible damage!");
        editDbConfirm
            .viewTask
            .done(() => {
                this.updatedDto['__metadata'] = { '@etag': this.document().__metadata['@etag'] };
                var newDoc = new document(this.updatedDto);
                var saveCommand = new saveDatabaseSettingsCommand(appUrl.getDatabase(), newDoc);
                var saveTask = saveCommand.execute();
                saveTask.done((idAndEtag: { Key: string; ETag: string }) => {
                    this.document().__metadata['@etag'] = idAndEtag.ETag;
                    this.docEditor.setReadOnly(true);
                    this.isEditingEnabled(false);
                    this.formatDocument();
                    viewModelBase.dirtyFlag().reset(); //Resync Changes
                });
            });
        app.showDialog(editDbConfirm);
    }

    refreshFromServer() {
        this.showRefreshAlert().done((answer) => {
            if (answer == undefined || answer == "Yes") {
                this.fetchDatabaseSettings(this.activeDatabase(), true)
                    .done(()=> {
                        this.isEditingEnabled(false);
                        viewModelBase.dirtyFlag().reset(); //Resync Changes
                    });
            }
        });
    }

    private showRefreshAlert(): any {
        var deferred = $.Deferred();
        var isDirty = viewModelBase.dirtyFlag().isDirty();
        if (isDirty) {
            return app.showMessage('You have unsaved data. Are you sure you want to refresh the data from the server?', 'Unsaved Data', ['Yes', 'No']);
        }
        return deferred.resolve();
    }

    private getDatabaseSettingsDocumentId(db: database) {
        return "Raven/Databases/" + db.name;
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