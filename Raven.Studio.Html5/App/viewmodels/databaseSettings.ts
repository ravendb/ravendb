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
    metadataText = ko.observable<string>("{}").extend({ required: true });
    text: KnockoutComputed<string>;
    docEditor: AceAjax.Editor;
    securedSettings: string;
    updatedDto: documentDto;
    textarea: any;
    isSaveEnabled: KnockoutComputed<Boolean>;
    isEditingEnabled = ko.observable<boolean>(false);
    isEditingMetadata = ko.observable<boolean>(false);

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
        this.isEditingMetadata.subscribe(()=> {
            if (this.docEditor) {
                var text = this.isEditingMetadata() ? this.metadataText() : this.documentText();
                this.docEditor.getSession().setValue(text);
            }
        });
        this.text = ko.computed({
            read: () => {
                return this.isEditingMetadata() ? this.metadataText() : this.documentText();
            },
            write: (text: string) => {
                var currentObservable = this.isEditingMetadata() ? this.metadataText : this.documentText;
                currentObservable(text);
            },
            owner: this
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

        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.documentText, this.metadataText]);
        this.isSaveEnabled = ko.computed(() => {
            return viewModelBase.dirtyFlag().isDirty();
        });
    }

    compositionComplete() {
        super.compositionComplete();
        this.docEditor = ko.utils.domData.get($("#dbDocEditor")[0], "aceEditor");
        this.textarea = $(this.docEditor.container).find('textarea')[0];

        this.subscribeToObservable(this.documentText, this.metadataText, "data", "metadata");
        this.subscribeToObservable(this.metadataText, this.documentText, "metadata", "data");
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

    refreshFromServer() {
        this.showRefreshAlert().done((answer) => {
            if (answer == "Yes") {
                this.fetchDatabaseSettings(this.activeDatabase(), true)
                    .done(() => {
                        this.metadataText("{}");
                        this.docEditor.setReadOnly(true);
                        this.isEditingEnabled(false);
                        this.activateDoc();
                        viewModelBase.dirtyFlag().reset(); //Resync Changes
                    });
            }
        });
    }

    formatDocument() {
        var text = this.isEditingMetadata() ? this.metadataText() : this.documentText();
        var observableToUpdate = this.isEditingMetadata() ? this.metadataText : this.documentText;
        try {
            var tempDoc = JSON.parse(text);
            var formatted = this.stringify(tempDoc);
            observableToUpdate(formatted);
        }
        catch (e) { }
    }

    activateMeta() {
        this.isEditingMetadata(true);
    }

    activateDoc() {
        this.isEditingMetadata(false);
    }

    saveChanges() {
        var editDbConfirm = new viewSystemDatabaseConfirm("Meddling with the database settings document could cause irreversible damage!");
        editDbConfirm
            .viewTask
            .done(() => {
                var updatedDto: documentDto = JSON.parse(this.documentText());
                var meta = JSON.parse(this.metadataText());
                updatedDto['@metadata'] = meta;
                updatedDto['@metadata']['@etag'] = this.document().__metadata['@etag'];
                var newDoc = new document(updatedDto);
                var saveCommand = new saveDatabaseSettingsCommand(appUrl.getDatabase(), newDoc);
                var saveTask = saveCommand.execute();
                saveTask.done((idAndEtag: { Key: string; ETag: string }) => {
                    this.document().__metadata['@etag'] = idAndEtag.ETag;
                    this.metadataText("{}");
                    this.docEditor.setReadOnly(true);
                    this.isEditingEnabled(false);
                    this.formatDocument();
                    viewModelBase.dirtyFlag().reset(); //Resync Changes
                });
            });
        app.showDialog(editDbConfirm);
    }

    private fetchDatabaseSettings(db: database, reportFetchProgress: boolean = false): JQueryPromise<any> {
        return new getDatabaseSettingsCommand(db, reportFetchProgress)
            .execute()
            .done((document: document) => { this.document(document); });
    }


    private subscribeToObservable(subscribedObservable, secondObservable, textType1: string, textType2: string) {
        subscribedObservable.subscribe(text=> {
            var message = "";
            try {
                var text1 = JSON.parse(text);
                var text2 = JSON.parse(secondObservable());
            }
            catch (e1) {
                if (text1 == undefined) {
                    message = "The " + textType1 + " isn't a legal JSON expression!";
                }
                else if (text2 == undefined) {
                    message = "The " + textType2 + " isn't a legal JSON expression!";
                }
            }
            finally {
                this.textarea.setCustomValidity(message);
            }
        });
    }

    private showRefreshAlert(): any {
        var deferred = $.Deferred();
        var isDirty = viewModelBase.dirtyFlag().isDirty();
        if (isDirty) {
            return app.showMessage('You have unsaved data. Are you sure you want to refresh the data from the server?', 'Unsaved Data', ['Yes', 'No']);
        }
        return deferred.resolve("Yes");
    }

    private getDatabaseSettingsDocumentId(db: database) {
        return "Raven/Databases/" + db.name;
    }

    private stringify(obj: any) {
        var prettifySpacing = 4;
        return JSON.stringify(obj, null, prettifySpacing);
    }
}

export = databaseSettings;