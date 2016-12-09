import chai = require("chai");
import utils = require("utils");

var viewUnderTest = 'database/status/statistics';

describe(viewUnderTest, () => {

    utils.initTest();

    it('should bind', () => {
        utils.mockCommand('commands/resources/getDatabaseStatsCommand', getTestData);
        utils.mockCommand('commands/database/index/getIndexesStatsCommand', getIndexStats);

        return utils.runViewmodelTest(viewUnderTest, {});
    });
});

function getIndexStats(): Raven.Client.Data.Indexes.IndexStats[] {
    return [
        {
            "Collections": {
                "Orders": {
                    "DocumentLag": 0,
                    "LastProcessedDocumentEtag": 977,
                    "LastProcessedTombstoneEtag": 0,
                    "TombstoneLag": 0
                }
            },
            "CreatedTimestamp": "2016-12-09T08:46:00.7684940Z",
            "EntriesCount": 89,
            "ErrorsCount": 0,
            "Id": 1,
            "IsInvalidIndex": false,
            "IsStale": false,
            "IsTestIndex": false,
            "LastBatchStats": {
                "DurationInMilliseconds": 68.15,
                "FailedCount": 0,
                "InputCount": 0,
                "OutputCount": 0,
                "Started": "2016-12-09T10:27:37.4845274Z",
                "SuccessCount": 0
            },
            "LastIndexingTime": "2016-12-09T10:27:37.4845274Z",
            "LastQueryingTime": "2016-12-09T10:27:37.4624679Z",
            "LockMode": "Unlock",
            "MapAttempts": 830,
            "MapErrors": 0,
            "MapSuccesses": 830,
            "MappedPerSecondRate": 0.0,
            "Memory": {
                "DiskSize": { "HumaneSize": "1.19 MBytes", "SizeInBytes": 1245184 },
                "MemoryBudget": { "HumaneSize": "32 MBytes", "SizeInBytes": 33554432 },
                "ThreadAllocations": { "HumaneSize": "0 Bytes", "SizeInBytes": 0 }
            },
            "Name": "Orders/ByCompany",
            "Priority": "Normal",
            "ReduceAttempts": 830,
            "ReduceErrors": 0,
            "ReduceSuccesses": 830,
            "ReducedPerSecondRate": 0.0,
            "State": "Normal",
            "Status": "Running",
            "Type": "MapReduce"
        }
    ];
}

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