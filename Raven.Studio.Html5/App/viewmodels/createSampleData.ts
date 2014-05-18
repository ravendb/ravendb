import getStatisticsCommand = require("commands/getDatabaseStatsCommand");
import createSampleDataCommand = require("commands/createSampleDataCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class createSampleData extends viewModelBase{
    isBusy = ko.observable(false);

    generateSampleData() {
        this.isBusy(true);
        new createSampleDataCommand(this.activeDatabase())
            .execute()
            .always(() => this.isBusy(false));
    }
}


export = createSampleData; 