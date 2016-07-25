import viewModelBase = require("viewmodels/viewModelBase");
import app = require("durandal/app");
import createSampleDataCommand = require("commands/database/studio/createSampleDataCommand");
import createSampleDataClassCommand = require("commands/database/studio/createSampleDataClassCommand");
import showDataDialog = require("viewmodels/common/showDataDialog");

class createSampleData extends viewModelBase{

    isBusy = ko.observable(false);
    isEnable = ko.observable(true);
    isVisible =  ko.observable(false);
    classData = ko.observable<string>();

    generateSampleData() {
        this.isBusy(true);
        
        new createSampleDataCommand(this.activeDatabase())
            .execute()
            .always(() => this.isBusy(false));
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('OGRN53');
    }

    showSampleDataClass() {
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
