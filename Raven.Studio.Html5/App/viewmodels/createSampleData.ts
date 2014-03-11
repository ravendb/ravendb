import getStatisticsCommand = require("commands/getDatabaseStatsCommand");
import createSampleDataCommand = require("commands/createSampleDataCommand");

class createSampleData {

    generateSampleData() {
        var createSampleDataCmd = new createSampleDataCommand().execute();
    }
}


export = createSampleData; 