import utils = require("utils");

var viewUnderTest = 'database/indexes/indexPerformanceDetails';

describe(viewUnderTest, () => {
    utils.initTest();

    it('should bind', () => {

        return utils.mockActiveDatabase()
            .then(() => utils.runViewmodelTest(viewUnderTest, {
                viewmodelConstructorArgs: [getIndexPerformance()]
            }));
    });

});


function getIndexPerformance(): Raven.Client.Data.Indexes.IndexingPerformanceOperation {
    return {
        "CommitDetails": {
            "NumberOfModifiedPages": 0,
            "NumberOfPagesWrittenToDisk": 0
        },
        "DurationInMilliseconds": 99.77,
        "MapDetails": {
            "AllocationBudget": 0,
            "BatchCompleteReason": "No more documents to index",
            "CurrentlyAllocated": 0,
            "ProcessPrivateMemory": 0,
            "ProcessWorkingSet": 0
        },
        "Name": "Indexing",
        Operations: [],
        ReduceDetails: {
            "NumberOfCompressedLeafs": 0,
            "NumberOfModifiedBranches": 0,
            "NumberOfModifiedLeafs": 3
        }
    }
}
