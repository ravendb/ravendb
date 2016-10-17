import viewmodelBase = require("viewmodels/viewModelBase");
import saveCsvFileCommand = require("commands/database/studio/saveCsvFileCommand");
import eventsCollector = require("common/eventsCollector");

class csvImport extends viewmodelBase {
    
    hasFileSelected = ko.observable(false);
    isImporting = ko.observable(false);

    fileSelected(args: any) {
        this.hasFileSelected(true);
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('CX96R3');
    }

    importCsv() {
        eventsCollector.default.reportEvent("csv", "import");
        if (!this.isImporting()) {
            this.isImporting(true);

            var formData = new FormData();
            var fileInput = <HTMLInputElement>document.querySelector("#csvFilePicker");
            formData.append("file", fileInput.files[0]);

            new saveCsvFileCommand(formData, fileInput.files[0].name, this.activeDatabase())
                .execute()
                .always(() => this.isImporting(false));
        }
    }

}

export = csvImport; 
