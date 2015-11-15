import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/database");
import getOperationStatusCommand = require("commands/getOperationStatusCommand");
import messagePublisher = require("common/messagePublisher");
import importDatabaseCommand = require("commands/importDatabaseCommand");

class importDatabase extends viewModelBase {
    showAdvancedOptions = ko.observable(false);
    filters = ko.observableArray<filterSettingDto>();
    batchSize = ko.observable(1024);
    includeExpiredDocuments = ko.observable(true);
    stripReplicationInformation = ko.observable(false);
    transformScript = ko.observable<string>();
    includeDocuments = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeAttachments = ko.observable(false);
    includeTransformers = ko.observable(true);
    removeAnalyzers = ko.observable(false);
    hasFileSelected = ko.observable(false);
    importedFileName = ko.observable<string>();
    isUploading = false;
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

        var db: database = this.activeDatabase();
        var importStatus = db.importStatus();
        if (importStatus === "Uploading 100%") {
            db.importStatus("");
        }
    }

    canDeactivate(isClose) {
        super.canDeactivate(isClose);

        if (this.isUploading) {
            this.confirmationMessage("Upload is in progress", "Please wait until uploading is complete.", ["OK"]);
            return false;
        }
        return true;
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            ko.postbox.subscribe("UploadProgress", (percentComplete: number) => this.activeDatabase().importStatus("Uploading " + percentComplete.toFixed(2).replace(/\.0*$/, "") + "%")),
            ko.postbox.subscribe("ChangesApiReconnected", (db: database) => {
                db.importStatus("");
                db.isImporting(false);
                this.isUploading = false;
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
        var isFileSelected = !!$.trim(fileName);
        this.hasFileSelected(isFileSelected);
        this.importedFileName($(this.filePickerTag).val().split(/(\\|\/)/g).pop());

        var db: database = this.activeDatabase();
        db.importStatus("");
    }

    importDb() {
        var db: database = this.activeDatabase();
        db.isImporting(true);
        this.isUploading = true;
        

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
                
        new importDatabaseCommand(formData, this.batchSize(), this.includeExpiredDocuments(), this.stripReplicationInformation(), importItemTypes, this.filters(), this.transformScript(), this.activeDatabase())
            .execute()
            .done((result: operationIdDto) => {
                var operationId = result.OperationId;
                this.waitForOperationToComplete(db, operationId);
                db.importStatus("Processing uploaded file");
            })
            .fail(() => db.importStatus(""))
            .always(() => this.isUploading = false);
    }

    private waitForOperationToComplete(db: database, operationId: number) {        
        new getOperationStatusCommand(db, operationId)
            .execute()
            .done((result: importOperationStatusDto) => this.importStatusRetrieved(db, operationId, result));
    }

    private importStatusRetrieved(db: database, operationId: number, result: importOperationStatusDto) {
        if (result.Completed) {
            if (result.ExceptionDetails == null && result.LastProgress != null) {
                this.hasFileSelected(false);
                $(this.filePickerTag).val('');
                db.importStatus("Last import was from '" + this.importedFileName() + "', " + result.LastProgress.toLocaleLowerCase());
                messagePublisher.reportSuccess("Successfully imported data to " + db.name);
            } else {
                db.importStatus("");
                messagePublisher.reportError("Failed to import data!", result.ExceptionDetails);
            }
            db.isImporting(false);
        }
        else {
            if (!!result.LastProgress) {
                db.importStatus("Processing uploaded file, " + result.LastProgress.toLocaleLowerCase());
            }
            setTimeout(() => this.waitForOperationToComplete(db, operationId), 1000);
        }
    }
}

export = importDatabase; 
