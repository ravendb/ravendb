import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class visualizerImport extends dialogViewModelBase {

    private importTask = $.Deferred();
    
    hasFileSelected = ko.observable(false);
    isImporting = ko.observable(false);

    fileSelected(args: any) {
        this.hasFileSelected(true);
    } 

    doImport() {
        var fileInput = <HTMLInputElement>document.querySelector("#importFilePicker");
        var self = this;
        var file = fileInput.files[0];
        var reader = new FileReader();
        reader.onload = function() {
            self.dataImported(this.result);
        };
        reader.onerror = (error) => this.importTask.reject(error);
        reader.readAsText(file);
    }

    dataImported(result: any) {
        var json = JSON.parse(result);
        this.importTask.resolve(json);
        dialog.close(this);
    }

    cancel() {
        dialog.close(this);
    }

    task() {
        return this.importTask.promise();
    }

}

export = visualizerImport;
