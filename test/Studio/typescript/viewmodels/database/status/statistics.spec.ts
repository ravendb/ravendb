import chai = require("chai");
import utils = require("utils");

var viewUnderTest = 'database/status/statistics';

describe(viewUnderTest, () => {

    utils.initTest();

    it('should bind', () => {
        utils.mockCommand('commands/resources/getDatabaseStatsCommand', getTestData);

        return utils.runViewmodelTest(viewUnderTest, {});
    });
});


function getTestData(): Raven.Client.Data.DatabaseStatistics {
    return {
        StaleIndexes: [],
        "CountOfIndexes": 3,
        "CountOfDocuments": 1059,
        "CountOfTransformers": 1,
        "DatabaseId": "0acab7bf-e569-463d-845f-3514902788ed",
        "Is64Bit": true,
        "LastDocEtag": 1059,
        "LastIndexingTime": "2016-12-03T19:55:22.1995331Z",
        "Indexes": [
            {
                "IsStale": false,
                "Name": "Orders/ByCompany",
                "IndexId": 1,
                "LockMode": "Unlock",
                "State": "Normal",
                "Type": "MapReduce",
                "LastIndexingTime": "2016-12-03T19:55:22.1925141Z"
            },
            {
                "IsStale": false,
                "Name": "Orders/Totals",
                "IndexId": 2,
                "LockMode": "Unlock",
                "State": "Normal",
                "Type": "Map",
                "LastIndexingTime": "2016-12-03T19:55:22.1864985Z"
            },
            {
                "IsStale": false,
                "Name": "Product/Sales",
                "IndexId": 3,
                "LockMode": "Unlock",
                "State": "Normal",
                "Type": "MapReduce",
                "LastIndexingTime": "2016-12-03T19:55:22.1995331Z"
            }
        ]
    }

}