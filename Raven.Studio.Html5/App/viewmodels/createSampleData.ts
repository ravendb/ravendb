import getStatisticsCommand = require("commands/getDatabaseStatsCommand");
import createSampleDataCommand = require("commands/createSampleDataCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class createSampleData extends viewModelBase{

    generateSampleData() {
        var createSampleDataCmd = new createSampleDataCommand(this.activeDatabase()).execute();
    }
}


export = createSampleData; 