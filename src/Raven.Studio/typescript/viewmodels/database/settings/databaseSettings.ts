import app = require("durandal/app");
import appUrl = require("common/appUrl");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import getDatabaseSettingsCommand = require("commands/resources/getDatabaseSettingsCommand");
import saveDatabaseSettingsCommand = require("commands/resources/saveDatabaseSettingsCommand");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import jsonUtil = require("common/jsonUtil");
import viewModelBase = require("viewmodels/viewModelBase");
import viewSystemDatabaseConfirm = require("viewmodels/common/viewSystemDatabaseConfirm");
import messagePublisher = require("common/messagePublisher");
import shell = require('viewmodels/shell');

class databaseSettings extends viewModelBase {
    document = ko.observable<document>();
    documentText = ko.observable<string>().extend({ required: true });
    metadataText = ko.observable<string>("{}").extend({ required: true });
    text: KnockoutComputed<string>;
    docEditor: AceAjax.Editor;
    securedSettings: string;
    updatedDto: documentDto;
    isSaveEnabled: KnockoutComputed<Boolean>;
    isEditingEnabled = ko.observable<boolean>(false);
    isEditingMetadata = ko.observable<boolean>(false);
    leavePageDeffered: JQueryPromise<any>;
    isForbidden = ko.observable<boolean>(false);

    static containerId ="#databaseSettingsContainer";

    constructor() {
        super();
        viewModelBase.layout.setMode(true);
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

    canActivate(args: any) {
        super.canActivate(args);
        var deferred = $.Deferred();

        this.isForbidden(!shell.isGlobalAdmin());
        if (this.isForbidden()) {
            deferred.resolve({ can: true });
        } else {
            var db: database = this.activeDatabase();
            this.fetchDatabaseSettings(db)
                .done(() => deferred.resolve({ can: true }))
                .fail((response: JQueryXHR) => {
                    messagePublisher.reportError("Error fetching database document!", response.responseText, response.statusText);
                    deferred.resolve({ redirect: appUrl.forStatus(db) });
                });
        }

        return deferred;
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('3QMLGH');
        this.dirtyFlag = new ko.DirtyFlag([this.documentText, this.metadataText], false, jsonUtil.newLineNormalizingHashFunction);
        this.isSaveEnabled = ko.computed(() => this.dirtyFlag().isDirty());
    }

    compositionComplete() {
        super.compositionComplete();

        var editorElement = $("#dbDocEditor");
        if (editorElement.length > 0)
        {
            this.docEditor = ko.utils.domData.get(editorElement[0], "aceEditor");
        }
        $("#dbDocEditor").on('DynamicHeightSet', () => this.docEditor.resize());
    }

    detached() {
        super.detached();
        $("#dbDocEditor").off('DynamicHeightSet');
    }

    editDatabaseSettings() {
        var editDbConfirm = new viewSystemDatabaseConfirm("Meddling with the database settings document could cause irreversible damage!");
        editDbConfirm
            .viewTask
            .done(() => {
                this.isEditingEnabled(true);
            });
        app.showDialog(editDbConfirm);
    }

    refreshFromServer() {
        var canContinue = this.canContinueIfNotDirty('Unsaved Data', 'You have unsaved data. Are you sure you want to refresh the data from the server?');
        canContinue.done(() => {
            this.fetchDatabaseSettings(this.activeDatabase(), true)
                .done(() => {
                    this.metadataText("{}");
                    this.isEditingEnabled(false);
                    this.activateDoc();
                    this.dirtyFlag().reset(); //Resync Changes
                });
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
                try {
                    var updatedDto: documentDto = JSON.parse(this.documentText());
                    var meta = JSON.parse(this.metadataText());

                    updatedDto['@metadata'] = meta;
                    updatedDto['@metadata']['@etag'] = (<any>this.document()).__metadata['@etag'];
                    var newDoc = new document(updatedDto);
                    var saveCommand = new saveDatabaseSettingsCommand(appUrl.getDatabase(), newDoc);
                    var saveTask = saveCommand.execute();
                    saveTask.done((saveResult: databaseDocumentSaveDto) => {
                        (<any>this.document()).__metadata['@etag'] = saveResult.ETag;
                        this.metadataText("{}");
                        this.isEditingEnabled(false);
                        this.formatDocument();
                        this.dirtyFlag().reset(); //Resync Changes
                    });
                }
                catch (e) {
                    var message = "";
                    if (updatedDto == undefined) {
                        message = "The data isn't a legal JSON expression!";
                        this.isEditingMetadata(false);
                    }
                    else if (meta == undefined) {
                        message = "The metadata isn't a legal JSON expression!";
                        this.isEditingMetadata(true);
                    }
                    this.docEditor.focus();
                    messagePublisher.reportError(message);
                }

            });
        app.showDialog(editDbConfirm);
    }

    private fetchDatabaseSettings(db: database, reportFetchProgress: boolean = false): JQueryPromise<any> {
        return new getDatabaseSettingsCommand(db, reportFetchProgress)
            .execute()
            .done((document: document) => this.document(document));
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
