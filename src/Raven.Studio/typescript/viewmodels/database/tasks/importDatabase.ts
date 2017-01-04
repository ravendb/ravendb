import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");
import importDatabaseCommand = require("commands/database/studio/importDatabaseCommand");
import importDatabaseModel = require("models/database/tasks/importDatabaseModel");
import notificationCenter = require("common/notifications/notificationCenter");
import eventsCollector = require("common/eventsCollector");
import copyToClipboard = require("common/copyToClipboard");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import EVENTS = require("common/constants/events");

class importDatabase extends viewModelBase {

    private static readonly filePickerTag = "#importDatabaseFilePicker";

    model = new importDatabaseModel();

    static isImporting = ko.observable(false);
    isImporting = importDatabase.isImporting;

    showAdvancedOptions = ko.observable(false);
    showTransformScript = ko.observable(false);

    hasFileSelected = ko.observable(false);
    importedFileName = ko.observable<string>();

    isUploading = ko.observable<boolean>(false);
    uploadStatus = ko.observable<number>();

    importCommand: KnockoutComputed<string>;

    validationGroup = ko.validatedObservable({
        importedFileName: this.importedFileName
    });

    constructor() {
        super();

        this.bindToCurrentInstance("copyCommandToClipboard", "fileSelected");

        aceEditorBindingHandler.install();
        this.isUploading.subscribe(v => {
            if (!v) {
                this.uploadStatus(0);
            }
        });

        //TODO: implement this command
        this.importCommand = ko.pureComputed(() => 'Raven.Smuggler out http://live-test.ravendb.net raven.dump --operate-on-types=Documents,Indexes,Transformers --database="Media" --batch-size=1024 --excludeexpired');
        this.setupValidation();
    }

    private setupValidation() {
        this.importedFileName.extend({
            required: true,
            validation: [{
                validator: (name: string) => name && name.endsWith(".ravendbdump"),
                message: "Invalid file extension."
            }]
        });
    }

    attached() {
        super.attached();
        $("#transformScriptPopover").popover({
            html: true,
            trigger: "hover",
            content: "Transform scripts are written in JavaScript. <br /><br/>Example:<pre><span class=\"code-keyword\">function</span>(doc) {<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">var</span> id = doc['@metadata']['@id'];<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">if</span> (id === 'orders/999')<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">return null</span>;<br /><br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">return</span> doc;<br />}</pre>"
        });
        this.updateHelpLink("YD9M1R");
    }

    canDeactivate(isClose: boolean) {
        super.canDeactivate(isClose);

        if (this.isUploading()) {
            this.confirmationMessage("Upload is in progress", "Please wait until uploading is complete.", ["OK"]);
            return false;
        }

        return true;
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            ko.postbox.subscribe("UploadProgress", (percentComplete: number) => {
                var db = this.activeDatabase();
                if (!db) {
                    return;
                }

                if (!this.isUploading()) {
                    return;
                }

                if (percentComplete === 100) {
                    setTimeout(() => this.isUploading(false), 700);
                }

                this.uploadStatus(percentComplete);
            }),
            ko.postbox.subscribe(EVENTS.ChangesApi.Reconnected, (db: database) => {
                this.isUploading(false);
            })
        ];
    }

    fileSelected(fileName: string) {
        const isFileSelected = fileName ? !!fileName.trim() : false;
        this.hasFileSelected(isFileSelected);
        this.importedFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);
    }

    importDb() {
        if (!this.isValid(this.validationGroup)) {
            return;
        }

        eventsCollector.default.reportEvent("database", "import");
        this.isUploading(true);
        
        const fileInput = document.querySelector(importDatabase.filePickerTag) as HTMLInputElement;
        const db = this.activeDatabase();

        $.when<any>(this.getNextOperationId(db), this.getAuthToken(db))
            .then(([operationId]: [number], [token]: [singleAuthToken]) => {
                notificationCenter.instance.monitorOperation(db, operationId);
                new importDatabaseCommand(db, operationId, token, fileInput.files[0], this.model)
                    .execute()
                    .always(() => this.isUploading(false));
            });
    }

    copyCommandToClipboard() {
        copyToClipboard.copy(this.importCommand(), "Command was copied to clipboard.");
    }

    private getNextOperationId(db: database): JQueryPromise<number> {
        return new getNextOperationId(db).execute()
            .fail((qXHR, textStatus, errorThrown) => {
                messagePublisher.reportError("Could not get next task id.", errorThrown);
                this.isUploading(false);
            });
    }

    private getAuthToken(db: database): JQueryPromise<singleAuthToken> {
        return new getSingleAuthTokenCommand(db).execute()
            .fail((qXHR, textStatus, errorThrown) => {
                messagePublisher.reportError("Could not get single auth token.", errorThrown);
                this.isUploading(false);
            });
    }

}

export = importDatabase; 
