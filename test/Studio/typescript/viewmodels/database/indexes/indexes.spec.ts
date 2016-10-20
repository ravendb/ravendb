import utils = require("utils");

var viewUnderTest = 'database/indexes/indexes';

describe(viewUnderTest, () => {
    utils.initTest();

    it('should bind empty list', () => {

        utils.mockCommand('commands/database/index/getIndexesStatsCommand', () => []);
        utils.mockCommand("commands/database/index/getIndexesStatusCommand", () => []);
        utils.mockCommand('commands/database/index/getPendingIndexReplacementsCommand', () => []);

        return utils.mockActiveDatabase(dbCtr => new dbCtr("default"))
            .then(() => utils.runViewmodelTest(viewUnderTest, {}));
    });

    it('should bind non-empty list', () => {

        utils.mockCommand('commands/database/index/getIndexesStatsCommand', () => getSampleIndexStats());
        utils.mockCommand("commands/database/index/getIndexesStatusCommand", () => getSampleIndexStatus());
        utils.mockCommand('commands/database/index/getPendingIndexReplacementsCommand', () => []);

        return utils.mockActiveDatabase(dbCtr => new dbCtr("default"))
            .then(() => utils.runViewmodelTest(viewUnderTest, {}));
    });

    it.skip('should bind side-by-side index list', () => { });

    it('should bind faulty index', () => {
        utils.mockCommand('commands/database/index/getIndexesStatsCommand', () => getFaultyIndexStats());
        utils.mockCommand("commands/database/index/getIndexesStatusCommand", () => getFaultyIndexStatus());
        utils.mockCommand('commands/database/index/getPendingIndexReplacementsCommand', () => []);

        return utils.mockActiveDatabase(dbCtr => new dbCtr("default"))
            .then(() => utils.runViewmodelTest(viewUnderTest, {}));
    });
});

function getFaultyIndexStatus(): Raven.Client.Data.Indexes.IndexingStatus {
    return {
        Status: "Running",
        Indexes: []
    }
}

function getFaultyIndexStats(): Raven.Client.Data.Indexes.IndexStats[] {
    return [
        {
            "IsStale": false,
            IsInvalidIndex: false,
            "Collections": null,
            "Memory": null,
            "LastIndexingTime": null,
            "LastQueryingTime": null,
            "LockMode": "Unlock",
            LastBatchStats: null as Raven.Client.Data.Indexes.IndexingPerformanceBasicStats,
            "Name": "Faulty/Indexes/8",
            "Priority": "None",
            "Type": "Faulty",
            "CreatedTimestamp": "0001-01-01T00:00:00.0000000Z",
            "EntriesCount": 0,
            "Id": 8,
            "MapAttempts": 0,
            "MapErrors": 0,
            "MapSuccesses": 0,
            "ReduceAttempts": null,
            "ReduceErrors": null,
            "ReduceSuccesses": null,
            "ErrorsCount": 0,
            MappedPerSecondRate: 0,
            ReducedPerSecondRate: 0,
            "IsTestIndex": false,
            Status: "Paused"
        }
    ];
}

function getSampleIndexStatus(): Raven.Client.Data.Indexes.IndexingStatus {
    return {
        Status: "Running",
        Indexes: [
            {
                Name: "Orders/Totals",
                Status: "Running"
            },
            {
                Name: "Product/Sales",
                Status: "Running"    
            },
            {
                Name: "Products/New",
                Status: "Running"
            }
        ]
    }
}

function getSampleIndexStats(): Raven.Client.Data.Indexes.IndexStats[] {
    return [
        {
            IsStale: false, 
            IsInvalidIndex: false,
            "Collections": {
                "Orders": {
                    "LastProcessedDocumentEtag": 977,
                    "LastProcessedTombstoneEtag": 0,
                    "DocumentLag": 0,
                    "TombstoneLag": 0
                }
            },
            "Memory": {
                "DiskSize": {
                    "SizeInBytes": 983040,
                    "HumaneSize": "960 KBytes"
                },
                "ThreadAllocations": {
                    "SizeInBytes": 2588672,
                    "HumaneSize": "2.47 MBytes"
                },
                "MemoryBudget": {
                    SizeInBytes: 123,
                    HumaneSize: "1232"
                }
            },
            LastBatchStats: null as Raven.Client.Data.Indexes.IndexingPerformanceBasicStats,
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
            MappedPerSecondRate: 0,
            ReducedPerSecondRate: 0,
            "ErrorsCount": 0,
            "IsTestIndex": false,
            Status: "Paused"
        },
        {
            IsStale: false,
            IsInvalidIndex: false, 
            "Collections": {
                "Orders": {
                    "LastProcessedDocumentEtag": 977,
                    "LastProcessedTombstoneEtag": 0,
                    "DocumentLag": 0,
                    "TombstoneLag": 0
                }
            },
            "Memory": {
                "DiskSize": {
                    "SizeInBytes": 1245184,
                    "HumaneSize": "1.19 MBytes"
                },
                "ThreadAllocations": {
                    "SizeInBytes": 2588672,
                    "HumaneSize": "2.47 MBytes"
                },
                "MemoryBudget": {
                    SizeInBytes: 123,
                    HumaneSize: "1232"
                }
            },
            LastBatchStats: null as Raven.Client.Data.Indexes.IndexingPerformanceBasicStats,
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
            MappedPerSecondRate: 0,
            ReducedPerSecondRate: 0,
            "ErrorsCount": 0,
            "IsTestIndex": false,
            Status: "Paused"
        },
        {
            IsStale: false,
            IsInvalidIndex: false,
            "Collections": {
                "OrderItems": {
                    "LastProcessedDocumentEtag": 0,
                    "LastProcessedTombstoneEtag": 0,
                    "DocumentLag": 0,
                    "TombstoneLag": 0
                }
            },
            LastBatchStats: null as Raven.Client.Data.Indexes.IndexingPerformanceBasicStats,
            "Memory": {
                "DiskSize": {
                    "SizeInBytes": 458752,
                    "HumaneSize": "448 KBytes"
                },
                "ThreadAllocations": {
                    "SizeInBytes": 2523136,
                    "HumaneSize": "2.41 MBytes"
                }, 
                "MemoryBudget": {
                    SizeInBytes: 123,
                    HumaneSize: "1232"
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
            MappedPerSecondRate: 0,
            ReducedPerSecondRate: 0,
            "ReduceSuccesses": null,
            "ErrorsCount": 0,
            "IsTestIndex": false,
            Status: "Paused"
        }
    ];
}