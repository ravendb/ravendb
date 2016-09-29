import utils = require("utils");

var viewUnderTest = 'database/indexes/indexes';

describe(viewUnderTest, () => {
    utils.initTest();

    it('should bind empty list', () => {

        utils.mockCommand('commands/database/index/getIndexStatsCommand', () => []);
        utils.mockCommand('commands/database/index/getPendingIndexReplacementsCommand', () => []);

        return utils.mockActiveDatabase(dbCtr => new dbCtr("default"))
            .then(() => utils.runViewmodelTest(viewUnderTest, {}));
    });

    it('should bind non-empty list', () => {

        utils.mockCommand('commands/database/index/getIndexStatsCommand', () => getSampleIndexStats());
        utils.mockCommand('commands/database/index/getPendingIndexReplacementsCommand', () => []);

        return utils.mockActiveDatabase(dbCtr => new dbCtr("default"))
            .then(() => utils.runViewmodelTest(viewUnderTest, {}));
    });

    it.skip('should bind side-by-side index list', () => { });

    it.skip('should bind faulty index', () => { });
});


function getSampleIndexStats(): Raven.Client.Data.Indexes.IndexStats[] {
    return [
        {
            "IsInMemory": false,
            IsInvalidIndex: false,
            "Collections": {
                "Orders": {
                    "LastProcessedDocumentEtag": 977,
                    "LastProcessedTombstoneEtag": 0,
                    "NumberOfDocumentsToProcess": 0,
                    "NumberOfTombstonesToProcess": 0,
                    "TotalNumberOfDocuments": 830,
                    "TotalNumberOfTombstones": 0
                }
            },
            "Memory": {
                "InMemory": false,
                "DiskSize": {
                    "SizeInBytes": 983040,
                    "HumaneSize": "960 KBytes"
                },
                "ThreadAllocations": {
                    "SizeInBytes": 2588672,
                    "HumaneSize": "2.47 MBytes"
                }
            },
            "LastIndexingTime": "2016-09-29T10:44:55.9350079Z",
            "LastQueryingTime": "2016-09-29T10:44:55.9199676Z",
            "LockMode": "LockedIgnore",
            "Name": "Orders/Totals",
            "Priority": "Normal",
            "Type": "Map",
            "CreatedTimestamp": "2016-09-27T12:54:29.1972120Z",
            "EntriesCount": 830,
            "Id": 2,
            "MapAttempts": 830,
            "MapErrors": 0,
            "MapSuccesses": 830,
            "ReduceAttempts": null,
            "ReduceErrors": null,
            "ReduceSuccesses": null,
            "ErrorsCount": 0,
            "IsTestIndex": false
        },
        {
            "IsInMemory": false,
            IsInvalidIndex: false, 
            "Collections": {
                "Orders": {
                    "LastProcessedDocumentEtag": 977,
                    "LastProcessedTombstoneEtag": 0,
                    "NumberOfDocumentsToProcess": 0,
                    "NumberOfTombstonesToProcess": 0,
                    "TotalNumberOfDocuments": 830,
                    "TotalNumberOfTombstones": 0
                }
            },
            "Memory": {
                "InMemory": false,
                "DiskSize": {
                    "SizeInBytes": 1245184,
                    "HumaneSize": "1.19 MBytes"
                },
                "ThreadAllocations": {
                    "SizeInBytes": 2588672,
                    "HumaneSize": "2.47 MBytes"
                }
            },
            "LastIndexingTime": "2016-09-29T10:44:56.0849063Z",
            "LastQueryingTime": "2016-09-29T10:44:56.0773863Z",
            "LockMode": "Unlock",
            "Name": "Product/Sales",
            "Priority": "Normal",
            "Type": "MapReduce",
            "CreatedTimestamp": "2016-09-27T12:54:29.3869179Z",
            "EntriesCount": 77,
            "Id": 3,
            "MapAttempts": 830,
            "MapErrors": 0,
            "MapSuccesses": 830,
            "ReduceAttempts": 2155,
            "ReduceErrors": 0,
            "ReduceSuccesses": 2155,
            "ErrorsCount": 0,
            "IsTestIndex": false
        },
        {
            "IsInMemory": false,
            IsInvalidIndex: false,
            "Collections": {
                "OrderItems": {
                    "LastProcessedDocumentEtag": 0,
                    "LastProcessedTombstoneEtag": 0,
                    "NumberOfDocumentsToProcess": 0,
                    "NumberOfTombstonesToProcess": 0,
                    "TotalNumberOfDocuments": 0,
                    "TotalNumberOfTombstones": 0
                }
            },
            "Memory": {
                "InMemory": false,
                "DiskSize": {
                    "SizeInBytes": 458752,
                    "HumaneSize": "448 KBytes"
                },
                "ThreadAllocations": {
                    "SizeInBytes": 2523136,
                    "HumaneSize": "2.41 MBytes"
                }
            },
            "LastIndexingTime": "2016-09-29T10:44:56.1480722Z",
            "LastQueryingTime": "2016-09-29T10:44:56.1184944Z",
            "LockMode": "LockedIgnore",
            "Name": "Products/New",
            "Priority": "Idle",
            "Type": "Map",
            "CreatedTimestamp": "2016-09-28T13:39:35.9437489Z",
            "EntriesCount": 0,
            "Id": 8,
            "MapAttempts": 0,
            "MapErrors": 0,
            "MapSuccesses": 0,
            "ReduceAttempts": null,
            "ReduceErrors": null,
            "ReduceSuccesses": null,
            "ErrorsCount": 0,
            "IsTestIndex": false
        }
    ];
}