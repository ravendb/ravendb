import utils = require("utils");

var viewUnderTest = 'database/indexes/indexPerformance';

describe(viewUnderTest, () => {
    utils.initTest();

    it('should bind', () => {

        utils.mockCommand('commands/database/debug/getIndexesPerformance', () => getIndexPerformance());

        return utils.mockActiveDatabase()
            .then(() => utils.runViewmodelTest(viewUnderTest, {
                activateArgs: () => ({
                    database: "db1"
                })
            }));
    });

});


function getIndexPerformance(): Raven.Client.Data.Indexes.IndexPerformanceStats[] {
    return [
        {
            "IndexName": "Product/Sales",
            "IndexId": 3,
            "Performance": [
                {
                    "Completed": "2016-12-15T08:49:50.6045291Z",
                    "Details": {
                        "CommitDetails": null,
                        "DurationInMilliseconds": 99.77,
                        "MapDetails": null,
                        "Name": "Indexing",
                        "Operations": [
                            {
                                "CommitDetails": null,
                                "DurationInMilliseconds": 9.57,
                                "MapDetails": null,
                                "Name": "Cleanup",
                                "Operations": [
                                    {
                                        "CommitDetails": null,
                                        "DurationInMilliseconds": 6.75,
                                        "MapDetails": null,
                                        "Name": "Collection_Orders",
                                        "Operations": [],
                                        "ReduceDetails": null
                                    }
                                ],
                                "ReduceDetails": null
                            },
                            {
                                "CommitDetails": null,
                                "DurationInMilliseconds": 14.46,
                                "MapDetails": {
                                    "AllocationBudget": 0,
                                    "BatchCompleteReason": "No more documents to index",
                                    "CurrentlyAllocated": 0,
                                    "ProcessPrivateMemory": 0,
                                    "ProcessWorkingSet": 0
                                },
                                "Name": "Map",
                                "Operations": [
                                    {
                                        "CommitDetails": null,
                                        "DurationInMilliseconds": 11.15,
                                        "MapDetails": null,
                                        "Name": "Collection_Orders",
                                        "Operations": [
                                            {
                                                "CommitDetails": null,
                                                "DurationInMilliseconds": 1.42,
                                                "MapDetails": null,
                                                "Name": "Storage/DocumentRead",
                                                "Operations": [],
                                                "ReduceDetails": null
                                            },
                                            {
                                                "CommitDetails": null,
                                                "DurationInMilliseconds": 0,
                                                "MapDetails": null,
                                                "Name": "Linq",
                                                "Operations": [],
                                                "ReduceDetails": null
                                            }
                                        ],
                                        "ReduceDetails": null
                                    }
                                ],
                                "ReduceDetails": null
                            },
                            {
                                "CommitDetails": null,
                                "DurationInMilliseconds": 13.1,
                                "MapDetails": null,
                                "Name": "Reduce",
                                "Operations": [],
                                "ReduceDetails": null
                            },
                            {
                                "CommitDetails": null,
                                "DurationInMilliseconds": 2.89,
                                "MapDetails": null,
                                "Name": "Storage/Commit",
                                "Operations": [],
                                "ReduceDetails": null
                            }
                        ],
                        "ReduceDetails": null
                    },
                    "DurationInMilliseconds": 99.77,
                    "FailedCount": 0,
                    "InputCount": 0,
                    "OutputCount": 0,
                    "Started": "2016-12-15T08:49:50.5045291Z",
                    "SuccessCount": 0
                }
            ]
        },
        {
            "IndexName": "Orders/ByCompany",
            "IndexId": 4,
            "Performance": [
                {
                    "Completed": "2016-12-15T08:49:50.6072565Z",
                    "Details": {
                        "CommitDetails": null,
                        "DurationInMilliseconds": 16.61,
                        "MapDetails": null,
                        "Name": "Indexing",
                        "Operations": [
                            {
                                "CommitDetails": null,
                                "DurationInMilliseconds": 0.21,
                                "MapDetails": null,
                                "Name": "Cleanup",
                                "Operations": [
                                    {
                                        "CommitDetails": null,
                                        "DurationInMilliseconds": 0.2,
                                        "MapDetails": null,
                                        "Name": "Collection_Orders",
                                        "Operations": [],
                                        "ReduceDetails": null
                                    }
                                ],
                                "ReduceDetails": null
                            },
                            {
                                "CommitDetails": null,
                                "DurationInMilliseconds": 0.76,
                                "MapDetails": {
                                    "AllocationBudget": 0,
                                    "BatchCompleteReason": "No more documents to index",
                                    "CurrentlyAllocated": 0,
                                    "ProcessPrivateMemory": 0,
                                    "ProcessWorkingSet": 0
                                },
                                "Name": "Map",
                                "Operations": [
                                    {
                                        "CommitDetails": null,
                                        "DurationInMilliseconds": 0.75,
                                        "MapDetails": null,
                                        "Name": "Collection_Orders",
                                        "Operations": [
                                            {
                                                "CommitDetails": null,
                                                "DurationInMilliseconds": 0.02,
                                                "MapDetails": null,
                                                "Name": "Storage/DocumentRead",
                                                "Operations": [],
                                                "ReduceDetails": null
                                            },
                                            {
                                                "CommitDetails": null,
                                                "DurationInMilliseconds": 0,
                                                "MapDetails": null,
                                                "Name": "Linq",
                                                "Operations": [],
                                                "ReduceDetails": null
                                            }
                                        ],
                                        "ReduceDetails": null
                                    }
                                ],
                                "ReduceDetails": null
                            },
                            {
                                "CommitDetails": null,
                                "DurationInMilliseconds": 0.01,
                                "MapDetails": null,
                                "Name": "Reduce",
                                "Operations": [],
                                "ReduceDetails": null
                            },
                            {
                                "CommitDetails": null,
                                "DurationInMilliseconds": 2.92,
                                "MapDetails": null,
                                "Name": "Storage/Commit",
                                "Operations": [],
                                "ReduceDetails": null
                            }
                        ],
                        "ReduceDetails": null
                    },
                    "DurationInMilliseconds": 16.61,
                    "FailedCount": 0,
                    "InputCount": 0,
                    "OutputCount": 0,
                    "Started": "2016-12-15T08:49:50.5902565Z",
                    "SuccessCount": 0
                }
            ]
        }
    ];
}
