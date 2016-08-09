import chai = require("chai");
import utils = require("utils");

import viewModel = require("src/Raven.Studio/typescript/viewmodels/database/status/statistics");

var viewUnderTest = 'database/status/statistics';

describe(viewUnderTest, () => {

    utils.initTest();

    it('should bind', () => {
        utils.mockCommand('commands/resources/getDatabaseStatsCommand', getTestData());

        return utils.runViewmodelTest(viewUnderTest, {
            afterAttach: () => {
                chai.expect($("#indexStatsAccordion").children().length).to.equal(1);
            }
        });
    });
});


function getTestData(): databaseStatisticsDto {
    var stats: any = {
        "LastDocEtag": "01000000-0000-0001-0000-000000000423",
        "CountOfIndexes": 4,
        "CountOfIndexesExcludingDisabledAndAbandoned": 4,
        "InMemoryIndexingQueueSizes": [0],
        "ApproximateTaskCount": 0,
        "CountOfDocuments": 1059,
        "StaleIndexes": [],
        "CountOfStaleIndexesExcludingDisabledAndAbandoned": 0,
        "CurrentNumberOfItemsToIndexInSingleBatch": 512,
        "CurrentNumberOfItemsToReduceInSingleBatch": 256,
        "DatabaseTransactionVersionSizeInMB": 0.00,
        "Indexes": <any>[
            {
                "Name": "Raven/DocumentsByEntityName",
                "IndexingAttempts": 1051,
                "IndexingSuccesses": 1051,
                "IndexingErrors": 0,
                "LastIndexedEtag": "01000000-0000-0001-0000-000000000423",
                "LastIndexedTimestamp": "2016-07-29T13:31:14.2351502Z",
                "LastQueryTimestamp": "2016-07-29T13:31:13.7195258Z",
                "TouchCount": 0,
                "Priority": "Normal",
                "ReduceIndexingAttempts": null,
                "ReduceIndexingSuccesses": null,
                "ReduceIndexingErrors": null,
                "LastReducedEtag": null,
                "LastReducedTimestamp": null,
                "CreatedTimestamp": "2016-07-29T13:31:09.4538982Z",
                "LastIndexingTime": "2016-07-29T13:31:15.3289009Z",
                "IsOnRam": "false",
                "LockMode": "LockedIgnore",
                "IsMapReduce": false,
                "ForEntityName": [],
                "DocsCount": 1051,
                "IsTestIndex": false,
                "IsInvalidIndex": false
            }
        ],
        "Errors": [],
        "Prefetches": [],
        "DatabaseId": "fd1ea6f8-bf60-4eda-8a4a-16ac6cab0b43",
        "SupportsDtc": true,
        "Is64Bit": true
    };

    return stats;

}