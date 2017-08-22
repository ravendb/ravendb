import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");
import importDatabaseCommand = require("commands/database/studio/importDatabaseCommand");
import importDatabaseModel = require("models/database/tasks/importDatabaseModel");
import notificationCenter = require("common/notifications/notificationCenter");
import eventsCollector = require("common/eventsCollector");
import appUrl = require("common/appUrl");
import copyToClipboard = require("common/copyToClipboard");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import EVENTS = require("common/constants/events");
import generalUtils = require("common/generalUtils");
import popoverUtils = require("common/popoverUtils");
import defaultAceCompleter = require("common/defaultAceCompleter");

class importDatabase extends viewModelBase {

    private static readonly filePickerTag = "#importDatabaseFilePicker";

    model = new importDatabaseModel();
    completer = defaultAceCompleter.completer();

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
        importedFileName: this.importedFileName,
        transformScript: this.model.transformScript
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
        this.showTransformScript.subscribe(v => {
            if (v) {
                this.model.transformScript("function transform(doc) {\r\n  var id = doc['@metadata']['@id'];\r\n  return doc;\r\n}");
            } else {
                this.model.transformScript("");
            }
        });

        //TODO: change input file name to be full document path

        this.importCommand = ko.pureComputed(() =>
            //TODO: review for smuggler.exe!
             {
                const db = this.activeDatabase();
                if (!db) {
                    return "";
                }

                const targetServer = appUrl.forServer();
                const model = this.model;
                const inputFilename = this.importedFileName() ? generalUtils.escapeForShell(this.importedFileName()) : "";
                const commandTokens = ["Raven.Smuggler", "in", targetServer, inputFilename];

                const databaseName = db.name;
                commandTokens.push("--database=" + generalUtils.escapeForShell(databaseName));

                const types: Array<string> = [];
                if (model.includeDocuments()) {
                    types.push("Documents");
                }
                if (model.includeRevisionDocuments()) {
                    types.push("RevisionDocuments");
                }
                if (model.includeIndexes()) {
                    types.push("Indexes");
                }
                if (model.includeIdentities()) {
                    types.push("Identities");
                }
                if (types.length > 0) {
                    commandTokens.push("--operate-on-types=" + types.join(","));
                }

                if (model.includeExpiredDocuments()) {
                    commandTokens.push("--include-expired");
                }

                if (model.removeAnalyzers()) {
                    commandTokens.push("--remove-analyzers");
                }

                if (model.transformScript() && this.showTransformScript()) {
                    commandTokens.push("--transform=" + generalUtils.escapeForShell(model.transformScript()));
                }

                return commandTokens.join(" ");
            });


        this.setupValidation();
    }

    private setupValidation() {
        this.importedFileName.extend({
            required: true
        });

        this.model.transformScript.extend({
            aceValidation: true
        });

    }

    attached() {
        super.attached();

        popoverUtils.longWithHover($(".scriptPopover"),
            {
                content:
                "<div class=\"text-center\">Transform scripts are written in JavaScript </div>" +
                "<pre><span class=\"token keyword\">function </span>transform(doc) " +
                "{<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"token keyword\">var</span> id = doc['@metadata']['@id'];<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" +
                "<span class=\"token keyword\">if</span> (id === 'orders/999')<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" +
                "<span class=\"token keyword\">return null</span>; <span class=\"token comment\">// filter-out</span><br /><br />" +
                "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"token keyword\">this</span>.Freight = <span class=\"token number\">15.3</span>;<br />" +
                "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"token keyword\">return</span> doc;<br />}</pre>"
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
                const db = this.activeDatabase();
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

        this.getNextOperationId(db)
            .done((operationId: number) => {
                notificationCenter.instance.openDetailsForOperationById(db, operationId);

                notificationCenter.instance.monitorOperation(db, operationId);

                new importDatabaseCommand(db, operationId, fileInput.files[0], this.model)
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

}

export = importDatabase; 
