import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

import eventsCollector = require("common/eventsCollector");

class exportDatabase extends viewModelBase {
    batchSize = ko.observable(1024);
    noneDefualtFileName = ko.observable<string>("");
    chooseDifferntFileName = ko.observable<boolean>(false);

    constructor() {
        super();
        aceEditorBindingHandler.install();
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("N822WN");
    }

    startExport() {
        eventsCollector.default.reportEvent("fs", "export");

        var fs = this.activeFilesystem();
        throw new Error("impl me!");
        /* TODO fs.isExporting(true);
        fs.exportStatus("");

        var smugglerOptions = {
            BatchSize: this.batchSize(),
            NoneDefualtFileName: this.noneDefualtFileName()
        };
        
        var url = "/studio-tasks/exportFilesystem";
        this.downloader.downloadByPost(fs, url, smugglerOptions, fs.isExporting, fs.exportStatus);*/
    }
}

export = exportDatabase;
