import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import jszip = require('jszip');

class importPackageImport extends dialogViewModelBase {

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
        reader.onload = function () {
            // try to detect type
            var firstChar = new Uint8Array(this.result.slice(0, 1))[0];
            if (firstChar === 91) {
                var rawJson = String.fromCharCode.apply(null, new Uint8Array(this.result));
                var json = JSON.parse(rawJson);
                self.dataImported(json); 
            } else {
                var zip = new jszip(this.result);
                var stacks = zip.file("stacktraces.txt");
                if (stacks) {
                    var stacksText = stacks.asText();
                    var stacksJson = JSON.parse(stacksText);
                    self.dataImported(stacksJson);
                } else {
                    self.dataImported(null);
                }
            }
        };
        reader.onerror = (error) => this.importTask.reject(error);
        reader.readAsArrayBuffer(file);
    }

    dataImported(result) {
        this.importTask.resolve(result);
        dialog.close(this);
    }

    cancel() {
        dialog.close(this);
    }

    task() {
        return this.importTask.promise();
    }
}

export = importPackageImport;
