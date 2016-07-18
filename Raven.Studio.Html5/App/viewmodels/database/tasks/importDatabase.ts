import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import getOperationStatusCommand = require("commands/operations/getOperationStatusCommand");
import messagePublisher = require("common/messagePublisher");
import importDatabaseCommand = require("commands/database/studio/importDatabaseCommand");
import checksufficientdiskspaceCommand = require("commands/database/studio/checksufficientdiskspaceCommand");

class importDatabase extends viewModelBase {
    showAdvancedOptions = ko.observable(false);
    filters = ko.observableArray<filterSettingDto>();
    batchSize = ko.observable(1024);
    includeExpiredDocuments = ko.observable(true);
    stripReplicationInformation = ko.observable(false);
    shouldDisableVersioningBundle = ko.observable(false);
    transformScript = ko.observable<string>();
    includeDocuments = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeAttachments = ko.observable(false);
    includeTransformers = ko.observable(true);
    removeAnalyzers = ko.observable(false);
    hasFileSelected = ko.observable(false);
    importedFileName = ko.observable<string>();
    isUploading = ko.observable<boolean>(false);
    private filePickerTag = "#importDatabaseFilePicker";

    constructor() {
        super();
        aceEditorBindingHandler.install();
    }

    attached() {
        super.attached();
        $("#transformScriptHelp").popover({
            html: true,
            trigger: "hover",
            content: "Transform scripts are written in JavaScript. <br /><br/>Example:<pre><span class=\"code-keyword\">function</span>(doc) {<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">var</span> id = doc['@metadata']['@id'];<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">if</span> (id === 'orders/999')<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">return null</span>;<br /><br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">return</span> doc;<br />}</pre>"
        });
        this.updateHelpLink("YD9M1R");
    }

    canDeactivate(isClose) {
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

                if (db.isImporting() === false || this.isUploading() === false) {
                    return;
                }

                db.importStatus("Uploading " + percentComplete.toFixed(2).replace(/\.0*$/, "") + "%");
            }),
            ko.postbox.subscribe("ChangesApiReconnected", (db: database) => {
                db.importStatus("");
                db.isImporting(false);
                this.isUploading(false);
            })
        ];
    }

    selectOptions() {
        this.showAdvancedOptions(false);
    }

    selectAdvancedOptions() {
        this.showAdvancedOptions(true);
    }

    removeFilter(filter: filterSettingDto) {
        this.filters.remove(filter);
    }

    addFilter() {
        var filter = {
            Path: "",
            ShouldMatch: false,
            ShouldMatchObservable: ko.observable(false),
            Values: []
        };

        filter.ShouldMatchObservable.subscribe(val => filter.ShouldMatch = val);
        this.filters.splice(0, 0, filter);
    }

    fileSelected(fileName: string) {        
        var db: database = this.activeDatabase();
        var isFileSelected = !!$.trim(fileName);
        var importFileName = $(this.filePickerTag).val().split(/(\\|\/)/g).pop();
        if (isFileSelected) {
            var fileInput = <HTMLInputElement>document.querySelector(this.filePickerTag);
            new checksufficientdiskspaceCommand(fileInput.files[0].size, this.activeDatabase())
                .execute()
                .done(() => {
                    this.hasFileSelected(isFileSelected);
                    this.importedFileName(importFileName);
                    db.importStatus("");
                })
                .fail(
                    () => {
                        db.importStatus("No sufficient diskspace for import, consider using Raven.Smuggler.exe directly.");
                        this.hasFileSelected(false);
                        this.importedFileName("");
                    }
                );
        }        
    }

    importDb() {
        var db: database = this.activeDatabase();
        db.isImporting(true);
        this.isUploading(true);
        
        var formData = new FormData();
        var fileInput = <HTMLInputElement>document.querySelector(this.filePickerTag);        
        formData.append("file", fileInput.files[0]);
        db.importStatus("Uploading 0%");
        var importItemTypes: ImportItemType[] = [];
        if (this.includeDocuments()) {
            importItemTypes.push(ImportItemType.Documents);
        }
        if (this.includeIndexes()) {
            importItemTypes.push(ImportItemType.Indexes);
        }
        if (this.includeAttachments()) {
            importItemTypes.push(ImportItemType.Attachments);
        }
        if (this.includeTransformers()) {
            importItemTypes.push(ImportItemType.Transformers);
        }
        if (this.removeAnalyzers()) {
            importItemTypes.push(ImportItemType.RemoveAnalyzers);
        }
                
        new importDatabaseCommand(formData, this.batchSize(), this.includeExpiredDocuments(), this.stripReplicationInformation(),this.shouldDisableVersioningBundle(),importItemTypes, this.filters(), this.transformScript(), this.activeDatabase())
            .execute()
            .done((result: operationIdDto) => {
                var operationId = result.OperationId;
                this.waitForOperationToComplete(db, operationId);
                db.importStatus("Processing uploaded file");
            })
            .fail(() => db.importStatus(""))
            .always(() => this.isUploading(false));
    }

    private waitForOperationToComplete(db: database, operationId: number) {        
        new getOperationStatusCommand(db, operationId)
            .execute()
            .done((result: dataDumperOperationStatusDto) => this.importStatusRetrieved(db, operationId, result));
    }

    private importStatusRetrieved(db: database, operationId: number, result: dataDumperOperationStatusDto) {
        if (result.Completed) {
            if (result.ExceptionDetails == null && result.State != null && result.State.Progress != null) {
                this.hasFileSelected(false);
                $(this.filePickerTag).val("");
                db.importStatus("Last import was from '" + this.importedFileName() + "', " + result.State.Progress.toLocaleLowerCase());
                messagePublisher.reportSuccess("Successfully imported data to " + db.name);
            } else if (result.Canceled) {
                db.importStatus("Import was canceled!");
            } else {
                db.importStatus("Failed to import database, see recent errors for details!");
                messagePublisher.reportError("Failed to import data!", result.ExceptionDetails);
            }

            db.isImporting(false);
        }
        else {
            if (!!result.State && result.State.Progress) {
                db.importStatus("Processing uploaded file, " + result.State.Progress.toLocaleLowerCase());
            }
            setTimeout(() => this.waitForOperationToComplete(db, operationId), 1000);
        }
    }
}

export = importDatabase; 
