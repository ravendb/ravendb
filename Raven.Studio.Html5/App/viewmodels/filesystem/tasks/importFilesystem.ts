import viewModelBase = require("viewmodels/viewModelBase");
import filesystem = require("models/filesystem/filesystem");
import messagePublisher = require("common/messagePublisher");
import importFilesystemCommand = require("commands/filesystem/importFilesystemCommand");
import getOperationStatusCommand = require("commands/operations/getOperationStatusCommand");
import fsCheckSufficientDiskSpaceCommand = require("commands/filesystem/fsCheckSufficientDiskSpaceCommand");

class importDatabase extends viewModelBase {
    batchSize = ko.observable(1024);
    stripReplicationInformation = ko.observable(false);
    shouldDisableVersioningBundle = ko.observable(false);
    hasFileSelected = ko.observable(false);
    importedFileName = ko.observable<string>();
    isUploading = false;
    private filePickerTag = "#importFilesystemFilePicker";

    attached() {
        super.attached();
        this.updateHelpLink("N822WN");

        var fs: filesystem = this.activeFilesystem();
        var importStatus = fs.importStatus();
        if (importStatus === "Uploading 100%") {
            fs.importStatus("");
        }
    }

    canDeactivate(isClose) {
        super.canDeactivate(isClose);
        
        if (this.isUploading) {
            this.confirmationMessage("Upload is in progress", "Please wait until uploading is complete.", ['OK']);
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
        var fs: filesystem = this.activeFilesystem();
        var isFileSelected = !!$.trim(fileName);
        var importFileName = $(this.filePickerTag).val().split(/(\\|\/)/g).pop();
        if (isFileSelected) {
            var fileInput = <HTMLInputElement>document.querySelector(this.filePickerTag);
            new fsCheckSufficientDiskSpaceCommand(fileInput.files[0].size, this.activeFilesystem())
                .execute()
                .done(() => {
                    this.hasFileSelected(isFileSelected);
                    this.importedFileName(importFileName);
                    fs.importStatus("");
                })
                .fail(() => {
                    fs.importStatus("No sufficient diskspace for import, consider using Raven.Smuggler.exe directly.");
                    this.hasFileSelected(false);
                    this.importedFileName("");
                });
        }
    }

    importFs() {
        var fs: filesystem = this.activeFilesystem();
        fs.isImporting(true);
        this.isUploading = true;
        fs.importStatus("Uploading 0%");

        var formData = new FormData();
        var fileInput = <HTMLInputElement>document.querySelector(this.filePickerTag);
        formData.append("file", fileInput.files[0]);
                
        new importFilesystemCommand(formData, this.batchSize(), this.stripReplicationInformation(), this.shouldDisableVersioningBundle(), this.activeFilesystem())
            .execute()
            .done((result: operationIdDto) => {
                var operationId = result.OperationId;
                this.waitForOperationToComplete(fs, operationId);
                fs.importStatus("Processing uploaded file");
            })
            .fail(() => {
                fs.importStatus("");
                fs.isImporting(false);
            })
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
                fs.importStatus("Last import was from '" + this.importedFileName());
                messagePublisher.reportSuccess("Successfully imported data to " + fs.name);
            } else {
                fs.importStatus("");
                messagePublisher.reportError("Failed to import data!", result.ExceptionDetails);
            }
            fs.isImporting(false);
        }
        else {
            if (result.State && result.State.Progress) {
                fs.importStatus("Processing uploaded file, " + result.State.Progress.toLocaleLowerCase());
            }
            setTimeout(() => this.waitForOperationToComplete(fs, operationId), 1000);
        }
    }
}

export = importDatabase; 
