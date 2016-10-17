import viewModelBase = require("viewmodels/viewModelBase");
import app = require("durandal/app");
import createSampleDataCommand = require("commands/database/studio/createSampleDataCommand");
import createSampleDataClassCommand = require("commands/database/studio/createSampleDataClassCommand");
import showDataDialog = require("viewmodels/common/showDataDialog");
import eventsCollector = require("common/eventsCollector");

class createSampleData extends viewModelBase{

    isBusy = ko.observable(false);
    isEnable = ko.observable(true);
    isVisible =  ko.observable(false);
    classData = ko.observable<string>();

    generateSampleData() {
        eventsCollector.default.reportEvent("sample-data", "create");
        this.isBusy(true);
        
        new createSampleDataCommand(this.activeDatabase())
            .execute()
            .always(() => this.isBusy(false));
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('OGRN53');
    }

    showSampleDataClass() {
        eventsCollector.default.reportEvent("sample-data-classes", "show");
        new createSampleDataClassCommand(this.activeDatabase())
            .execute()
            .done((results: string) => {
                this.isVisible(true);
                var data = results.replace("\r\n", "");

                app.showDialog(new showDataDialog("Sample Data Classes", data));
            })
            .always(() => this.isBusy(false));
    }
}

export = createSampleData; 
