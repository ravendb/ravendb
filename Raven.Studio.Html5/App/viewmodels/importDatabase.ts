import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/database");
import collection = require("models/collection");
import getOperationStatusCommand = require('commands/getOperationStatusCommand');
import messagePublisher = require("common/messagePublisher");

class importDatabase extends viewModelBase {
    showAdvancedOptions = ko.observable(false);
    filters = ko.observableArray<filterSettingDto>();
    batchSize = ko.observable(1024);
    includeExpiredDocuments = ko.observable(true);
    transformScript = ko.observable<string>();
    includeDocuments = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeAttachments = ko.observable(false);
    includeTransformers = ko.observable(true);
    removeAnalyzers = ko.observable(false);
    //includeAllCollections = ko.observable(true);
    //includedCollections = ko.observableArray<{ collection: string; isIncluded: KnockoutObservable<boolean>; }>();
    hasFileSelected = ko.observable(false);
    isUploading = false;
    private filePickerTag = "#importDatabaseFilePicker";

    constructor() {
        super();
        aceEditorBindingHandler.install();
    }

    attached() {
        $("#transformScriptHelp").popover({
            html: true,
            trigger: 'hover',
            content: 'Transform scripts are written in JavaScript. <br /><br/>Example:<pre><span class="code-keyword">var</span> company = LoadDocument(<span class="code-keyword">this</span>.Company);<br /><span class="code-keyword">if</span> (company) {<br />&nbsp;&nbsp;&nbsp;company.Orders = { <br /> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Count: <span class="code-keyword">this</span>.Count,<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Total: <span class="code-keyword">this</span>.Total<br />&nbsp;&nbsp;&nbsp;}<br /><br />&nbsp;&nbsp;&nbsp;PutDocument(<span class="code-keyword">this</span>.Company, company);<br />}</pre>',
        });
    }

    canDeactivate(isClose) {
        super.canDeactivate(isClose);

        if (this.isUploading) {
            this.confirmationMessage("Upload is in progress", "Please wait until uplodaing is complete.", ['OK']);
            return false;
        }
        return true;
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            ko.postbox.subscribe("UploadProgress", (percentComplete: number) => this.activeDatabase().importStatus("Uploading " + percentComplete.toFixed(2).replace(/\.0*$/, '') + "%")),
            ko.postbox.subscribe("ChangesApiReconnected", () => this.isUploading = false)
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
        this.filters.push({
            Path: "",
            ShouldMatch: false,
            Values: []
        });
    }

    fileSelected(fileName: string) {
        var isFileSelected = !!$.trim(fileName);
        this.hasFileSelected(isFileSelected);
    }

    importDb() {
        var db: database = this.activeDatabase();
        db.isImporting(true);
        this.isUploading = true;
        db.importStatus("Uploading 0%");

        var formData = new FormData();
        var fileInput = <HTMLInputElement>document.querySelector(this.filePickerTag);
        formData.append("file", fileInput.files[0]);
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

        require(["commands/importDatabaseCommand"], importDatabaseCommand => {
            new importDatabaseCommand(formData, this.batchSize(), this.includeExpiredDocuments(), importItemTypes, this.filters(), this.transformScript(), this.activeDatabase())
                .execute()
                .done((result: operationIdDto) => {
                    this.isUploading = false;
                    var operationId = result.OperationId;
                    this.waitForOperationToComplete(db, operationId);
                    db.importStatus("Processing uploaded file");
                });
        });
    }

    private waitForOperationToComplete(db: database, operationId: number) {
        
        var getOperationStatusTask = new getOperationStatusCommand(db, operationId);
        getOperationStatusTask.execute()
            .done((result: importOperationStatusDto) => {
                if (result.Completed) {
                    if (result.ExceptionDetails == null) {
                        this.hasFileSelected(false);
                        $(this.filePickerTag).val('');
                        messagePublisher.reportSuccess("Successfully imported data to " + db.name);
                    } else {
                        messagePublisher.reportError("Failed to import data!", result.ExceptionDetails);
                    }
                    db.isImporting(false);
                }
                else {
                    if (!!result.LastProgress) {
                        db.importStatus("Processing uploaded file, " + result.LastProgress.toLocaleLowerCase());
                    }
                    setTimeout(() => this.waitForOperationToComplete(db, operationId), 500);
                }
            });
    }
}

export = importDatabase; 