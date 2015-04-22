import viewModelBase = require("viewmodels/viewModelBase");
import filesystem = require("models/filesystem/filesystem");
import messagePublisher = require("common/messagePublisher");
import importFilesystemCommand = require("commands/filesystem/importFilesystemCommand");
import getOperationStatusCommand = require("commands/operations/getOperationStatusCommand");

class importDatabase extends viewModelBase {
    batchSize = ko.observable(1024);
    hasFileSelected = ko.observable(false);
    isUploading = false;
    private importedFileName: string;
    private filePickerTag = "#importFilesystemFilePicker";

    attached() {
        this.updateHelpLink("YD9M1R");
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
            ko.postbox.subscribe("UploadProgress", (percentComplete: number) => this.activeFilesystem().importStatus("Uploading " + percentComplete.toFixed(2).replace(/\.0*$/, '') + "%")),
            ko.postbox.subscribe("ChangesApiReconnected", (fs: filesystem) => {
                fs.importStatus("");
                fs.isImporting(false);
                this.isUploading = false;
            })
        ];
    }

    fileSelected(fileName: string) {
        var isFileSelected = !!$.trim(fileName);
        this.hasFileSelected(isFileSelected);
        this.importFs();
    }

    importFs() {
        var fs: filesystem = this.activeFilesystem();
        fs.isImporting(true);
        this.isUploading = true;
        fs.importStatus("Uploading 0%");

        var formData = new FormData();
        this.importedFileName = $(this.filePickerTag).val().split(/(\\|\/)/g).pop();
        var fileInput = <HTMLInputElement>document.querySelector(this.filePickerTag);
        formData.append("file", fileInput.files[0]);
                
        new importFilesystemCommand(formData, this.batchSize(), this.activeFilesystem())
            .execute()
            .done((result: operationIdDto) => {
                var operationId = result.OperationId;
                this.waitForOperationToComplete(fs, operationId);
                fs.importStatus("Processing uploaded file");
            })
            .fail(() => fs.importStatus(""))
            .always(() => this.isUploading = false);
    }

    private waitForOperationToComplete(fs: filesystem, operationId: number) {        
        new getOperationStatusCommand(fs, operationId)
            .execute()
            .done((result: importOperationStatusDto) => this.importStatusRetrieved(fs, operationId, result));
    }

    private importStatusRetrieved(fs: filesystem, operationId: number, result: importOperationStatusDto) {
        if (result.Completed) {
            if (result.ExceptionDetails == null) {
                this.hasFileSelected(false);
                $(this.filePickerTag).val('');
                fs.importStatus("Last import was from '" + this.importedFileName + "', " + result.LastProgress.toLocaleLowerCase());
                messagePublisher.reportSuccess("Successfully imported data to " + fs.name);
            } else {
                fs.importStatus("");
                messagePublisher.reportError("Failed to import data!", result.ExceptionDetails);
            }
            fs.isImporting(false);
        }
        else {
            if (!!result.LastProgress) {
                fs.importStatus("Processing uploaded file, " + result.LastProgress.toLocaleLowerCase());
            }
            setTimeout(() => this.waitForOperationToComplete(fs, operationId), 1000);
        }
    }
}

export = importDatabase; 