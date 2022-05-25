import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;

export class DatabaseStubs {
    static shardedDatabasesResponse() {
        const shard0 = {
            Name: "db$0",
        } as Raven.Client.ServerWide.Operations.DatabaseInfo;

        const shard1 = {
            Name: "db$1",
        } as Raven.Client.ServerWide.Operations.DatabaseInfo;

        const fakeResponse: Raven.Client.ServerWide.Operations.DatabasesInfo = {
            Databases: [shard0, shard1],
        };

        return fakeResponse;
    }

    static singleDatabaseResponse() {
        const db1 = {
            Name: "db1",
        } as Raven.Client.ServerWide.Operations.DatabaseInfo;

        const fakeResponse: Raven.Client.ServerWide.Operations.DatabasesInfo = {
            Databases: [db1],
        };

        return fakeResponse;
    }

    static essentialStats(): EssentialDatabaseStatistics {
        return {
            CountOfTimeSeriesSegments: 5,
            CountOfTombstones: 3,
            CountOfAttachments: 10,
            CountOfDocumentsConflicts: 5,
            CountOfRevisionDocuments: 12,
            CountOfDocuments: 1_234_567,
            CountOfIndexes: 17,
            CountOfCounterEntries: 1_453,
            CountOfConflicts: 83,
            Indexes: [],
        };
    }

    static detailedStats(): DetailedDatabaseStatistics {
        const essential = DatabaseStubs.essentialStats();
        return {
            ...essential,
            CountOfIdentities: 17,
            CountOfCompareExchange: 38,
            DatabaseChangeVector:
                "A:2568-F9I6Egqwm0Kz+K0oFVIR9Q, A:13366-IG4VwBTOnkqoT/uwgm2OQg, A:2568-OSKWIRBEDEGoAxbEIiFJeQ, A:8807-jMR/KF8hz0uMKFDXnmrQJA",
            CountOfTimeSeriesDeletedRanges: 9,
            Is64Bit: true,
            NumberOfTransactionMergerQueueOperations: 0,
            DatabaseId: "jMR/KF8hz0uMKFDXnmrQJA",
            CountOfCompareExchangeTombstones: 44,
            SizeOnDisk: {
                HumaneSize: "295.44 MBytes",
                SizeInBytes: 309788672,
            },
            TempBuffersSizeOnDisk: {
                HumaneSize: "17.19 MBytes",
                SizeInBytes: 18022400,
            },
            CountOfUniqueAttachments: essential.CountOfAttachments - 2,
            Pager: "Voron.Impl.Paging.RvnMemoryMapPager",
            StaleIndexes: [],
            Indexes: [],
        };
    }
}
