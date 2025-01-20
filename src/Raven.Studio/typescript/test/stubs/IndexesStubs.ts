import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import IndexProgress = Raven.Client.Documents.Indexes.IndexProgress;
import IndexUtils from "components/utils/IndexUtils";
import CollectionStats = Raven.Client.Documents.Indexes.IndexProgress.CollectionStats;
import IndexMergeResults = Raven.Server.Documents.Indexes.IndexMerging.IndexMergeResults;

const statsFixture = require("../fixtures/indexes_stats.json");
const progressFixture: { Results: IndexProgress[] } = require("../fixtures/indexes_progress.json");

export class IndexesStubs {
    static getSampleStats(): IndexStats[] {
        const stats: IndexStats[] = JSON.parse(JSON.stringify(statsFixture.Results));

        // create fake replacement index
        const ordersByCompany = stats.find((x) => x.Name === "Orders/ByCompany");
        const replacementOfOrders: IndexStats = JSON.parse(JSON.stringify(ordersByCompany));
        replacementOfOrders.Name = IndexUtils.SideBySideIndexPrefix + replacementOfOrders.Name;
        stats.push(replacementOfOrders);

        // set priority of 'Orders/ByCompany' to low and mode to lock error
        const ordersTotals = stats.find((x) => x.Name === "Orders/ByCompany");
        ordersTotals.Priority = "Low";
        ordersTotals.LockMode = "LockedError";

        // and set output reduce for collection
        ordersTotals.ReduceOutputCollection = "ByCompanyCollection";

        // order totals is indexed using Corax
        ordersTotals.SearchEngineType = "Corax";

        return stats;
    }

    static getSampleProgress(): IndexProgress[] {
        return JSON.parse(JSON.stringify(progressFixture.Results));
    }

    static getGenericStats(): IndexStats {
        return IndexesStubs.getSampleStats().find((x) => x.Name === "Orders/ByCompany");
    }

    static getGenericProgress(): IndexProgress {
        return IndexesStubs.getSampleProgress().find((x) => x.Name === "Orders/ByCompany");
    }

    static getUpToDateIndex(): [IndexStats, IndexProgress] {
        const stats = IndexesStubs.getGenericStats();
        stats.Name = "UpToDateIndex";

        return [stats, null];
    }

    static getUpToDateIndexWithErrors(): [IndexStats, IndexProgress] {
        const stats = IndexesStubs.getGenericStats();
        stats.Name = "UpToDateIndexWithErrors";
        stats.ErrorsCount = 5;

        return [stats, null];
    }

    static getStaleInProgressIndex(): [IndexStats, IndexProgress] {
        const stats = IndexesStubs.getGenericStats();
        stats.Name = "StaleInProgress";
        stats.IsStale = true;

        const progress = IndexesStubs.getGenericProgress();
        progress.IsStale = true;
        progress.Name = stats.Name;
        progress.ProcessedPerSecond = 2;
        progress.Collections = IndexesStubs.collectionStats();

        return [stats, progress];
    }

    static collectionStats(): { [key: string]: CollectionStats } {
        return {
            Orders: {
                LastProcessedItemEtag: 5,
                LastProcessedTombstoneEtag: 5,
                NumberOfItemsToProcess: 1000,
                NumberOfTombstonesToProcess: 2000,
                TotalNumberOfItems: 3000,
                TotalNumberOfTombstones: 5000,
                LastProcessedTimeSeriesDeletedRangeEtag: 5,
                NumberOfTimeSeriesDeletedRangesToProcess: 10,
                TotalNumberOfTimeSeriesDeletedRanges: 20,
            },
            SecondCollection: {
                LastProcessedItemEtag: 5,
                LastProcessedTombstoneEtag: 5,
                NumberOfItemsToProcess: 500,
                NumberOfTombstonesToProcess: 300,
                TotalNumberOfItems: 3000,
                TotalNumberOfTombstones: 5000,
                LastProcessedTimeSeriesDeletedRangeEtag: 5,
                NumberOfTimeSeriesDeletedRangesToProcess: 10,
                TotalNumberOfTimeSeriesDeletedRanges: 20,
            },
        };
    }

    static getDisabledIndexWithProgress(): [IndexStats, IndexProgress] {
        const stats = IndexesStubs.getGenericStats();
        stats.Name = "DisabledIndexWithProgress";
        stats.IsStale = true;
        stats.State = "Disabled";
        stats.Status = "Disabled";

        const progress = IndexesStubs.getGenericProgress();
        progress.IsStale = true;
        progress.Name = stats.Name;
        progress.Collections = IndexesStubs.collectionStats();
        return [stats, progress];
    }

    static getDisabledIndexWithOutProgress(): [IndexStats, IndexProgress] {
        const [stats] = IndexesStubs.getDisabledIndexWithProgress();
        stats.Name = "DisabledIndexWithoutProgress";

        return [stats, null];
    }

    static getPausedIndexWithProgress(): [IndexStats, IndexProgress] {
        const stats = IndexesStubs.getGenericStats();
        stats.Name = "PausedIndexWithProgress";
        stats.IsStale = true;
        stats.State = "Normal";
        stats.Status = "Paused";

        const progress = IndexesStubs.getGenericProgress();
        progress.IsStale = true;
        progress.Name = stats.Name;
        progress.Collections = IndexesStubs.collectionStats();
        return [stats, progress];
    }

    static getPausedIndexWithOutProgress(): [IndexStats, IndexProgress] {
        const [stats] = this.getPausedIndexWithProgress();
        stats.Name = "PausedIndexWithoutProgress";

        return [stats, null];
    }

    static getFaultyIndex(): [IndexStats, IndexProgress] {
        const stats = IndexesStubs.getGenericStats();
        stats.SourceType = "None";
        stats.Name = "FaultyIndex";
        stats.IsStale = true;
        stats.Type = "Faulty";
        stats.Collections = null;
        return [stats, null];
    }

    static getErroredIndex(): [IndexStats, IndexProgress] {
        const stats = IndexesStubs.getGenericStats();
        stats.Name = "ErroredIndex";
        stats.IsStale = true;
        stats.State = "Error";
        return [stats, null];
    }

    static getSampleMergeSuggestions(): IndexMergeResults {
        return {
            Unmergables: {
                "Orders/ByShipment/Location": "Cannot merge indexes that have a where clause",
                "Companies/StockPrices/TradeVolumeByMonth": "Cannot merge map/reduce indexes",
            },
            Suggestions: [
                {
                    CanMerge: ["Product/Search", "Products/ByUnitOnStock"],
                    CanDelete: null,
                    MergedIndex: {
                        SourceType: "Documents",
                        Priority: null,
                        State: null,
                        OutputReduceToCollection: null,
                        PatternForOutputReduceToCollectionReferences: null,
                        PatternReferencesCollectionName: null,
                        Name: "Product/Search",
                        Reduce: null,
                        Type: "Map",
                        Maps: [
                            "from doc in docs.Products\r\nselect new\r\n{\r\n    Category = doc.Category,\r\n    Name = doc.Name,\r\n    PricePerUnit = doc.PricePerUnit,\r\n    Supplier = doc.Supplier,\r\n    UnitOnStock = LoadCompareExchangeValue(Id(doc))\r\n}",
                        ],
                        Fields: {
                            Name: {
                                Analyzer: null,
                                Spatial: null,
                            },
                        },
                        Configuration: {},
                        AdditionalSources: {},
                        AdditionalAssemblies: [],
                        CompoundFields: [],
                    },
                    Collection: "Products",
                    SurpassingIndex: "",
                },
                {
                    CanMerge: null,
                    CanDelete: [
                        "Companies/StockPrices/TradeVolumeByMonth",
                        "Orders/ByCompany",
                        "Orders/ByShipment/Location",
                    ],
                    MergedIndex: null,
                    Collection: "Products",
                    SurpassingIndex: "Some/SurpassingIndexName",
                },
            ],
            Errors: [
                {
                    IndexName: "Companies/StockPrices/TradeVolumeByMonth",
                    Message: "Cannot merge map/reduce indexes",
                    StackTrace:
                        "at System.ThrowHelper.ThrowArgumentException(ExceptionResource resource) at System.Collections.Generic.Dictionary`2.Insert(TKey key, TValue value, Boolean add) at System.Collections.Generic.Dictionary`2.Add(TKey key, TValue value) at ExampleNamespace.ExampleClass.ExampleMethod()",
                },
                {
                    IndexName: "Orders/ByShipment/Location",
                    Message: "Cannot merge map/reduce indexes",
                    StackTrace:
                        "at System.ThrowHelper.ThrowArgumentException(ExceptionResource resource) at System.Collections.Generic.Dictionary`2.Insert(TKey key, TValue value, Boolean add) at System.Collections.Generic.Dictionary`2.Add(TKey key, TValue value) at ExampleNamespace.ExampleClass.ExampleMethod()",
                },
            ],
        };
    }

    static getIndexesErrorCount() {
        return {
            Results: [
                {
                    Name: "Orders/ByShipment/Location",
                    Errors: [],
                },
                {
                    Name: "Orders/Totals",
                    Errors: [],
                },
                {
                    Name: "Orders/ByCompany",
                    Errors: [
                        {
                            Action: "Map",
                            NumberOfErrors: 500,
                        },
                    ],
                },
                {
                    Name: "Product/Rating",
                    Errors: [],
                },
                {
                    Name: "Companies/StockPrices/TradeVolumeByMonth",
                    Errors: [],
                },
                {
                    Name: "Products/ByUnitOnStock",
                    Errors: [],
                },
                {
                    Name: "Product/Search",
                    Errors: [],
                },
            ],
        };
    }

    static getIndexErrorDetails() {
        return [
            {
                Name: "Orders/ByShipment/Location",
                Errors: [],
            },
            {
                Name: "Orders/Totals",
                Errors: [],
            },
            {
                Name: "Orders/ByCompany",
                Errors: [
                    {
                        Timestamp: "2025-01-28T10:24:20.6681287Z",
                        Document: "orders/13-A",
                        Action: "Map",
                        Error: "Failed to execute mapping function on orders/13-A. Exception: System.DivideByZeroException: Attempted to divide by zero.\r\n   at CallSite.Target(Closure, CallSite, Object, Object)\r\n   at Raven.Server.Documents.Indexes.Static.Generated.Index_Orders_ByCompany.Map_0(IEnumerable`1 docs)+MoveNext()\r\n   at Raven.Server.Documents.Indexes.Static.TimeCountingEnumerable.Enumerator.MoveNext() in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\Static\\TimeCountingEnumerable.cs:line 47\r\n   at Raven.Server.Documents.Indexes.MapIndexBase`2.UpdateIndexEntriesLucene(IndexItem indexItem, IEnumerable mapResults, Lazy`1 writer, TransactionOperationContext indexContext, IndexingStatsScope stats) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\MapIndexBase.cs:line 157\r\n   at Raven.Server.Documents.Indexes.MapIndexBase`2.HandleMap(IndexItem indexItem, IEnumerable mapResults, Lazy`1 writer, TransactionOperationContext indexContext, IndexingStatsScope stats) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\MapIndexBase.cs:line 59\r\n   at Raven.Server.Documents.Indexes.Workers.MapItems.Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext, Lazy`1 writeOperation, IndexingStatsScope stats, CancellationToken token) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\Workers\\MapItems.cs:line 143",
                    },
                    {
                        Timestamp: "2025-01-28T10:24:20.6681652Z",
                        Document: "orders/14-A",
                        Action: "Map",
                        Error: "Failed to execute mapping function on orders/14-A. Exception: System.DivideByZeroException: Attempted to divide by zero.\r\n   at CallSite.Target(Closure, CallSite, Object, Object)\r\n   at Raven.Server.Documents.Indexes.Static.Generated.Index_Orders_ByCompany.Map_0(IEnumerable`1 docs)+MoveNext()\r\n   at Raven.Server.Documents.Indexes.Static.TimeCountingEnumerable.Enumerator.MoveNext() in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\Static\\TimeCountingEnumerable.cs:line 47\r\n   at Raven.Server.Documents.Indexes.MapIndexBase`2.UpdateIndexEntriesLucene(IndexItem indexItem, IEnumerable mapResults, Lazy`1 writer, TransactionOperationContext indexContext, IndexingStatsScope stats) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\MapIndexBase.cs:line 157\r\n   at Raven.Server.Documents.Indexes.MapIndexBase`2.HandleMap(IndexItem indexItem, IEnumerable mapResults, Lazy`1 writer, TransactionOperationContext indexContext, IndexingStatsScope stats) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\MapIndexBase.cs:line 59\r\n   at Raven.Server.Documents.Indexes.Workers.MapItems.Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext, Lazy`1 writeOperation, IndexingStatsScope stats, CancellationToken token) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\Workers\\MapItems.cs:line 143",
                    },
                    {
                        Timestamp: "2025-01-28T10:24:20.6681991Z",
                        Document: "orders/15-A",
                        Action: "Map",
                        Error: "Failed to execute mapping function on orders/15-A. Exception: System.DivideByZeroException: Attempted to divide by zero.\r\n   at CallSite.Target(Closure, CallSite, Object, Object)\r\n   at Raven.Server.Documents.Indexes.Static.Generated.Index_Orders_ByCompany.Map_0(IEnumerable`1 docs)+MoveNext()\r\n   at Raven.Server.Documents.Indexes.Static.TimeCountingEnumerable.Enumerator.MoveNext() in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\Static\\TimeCountingEnumerable.cs:line 47\r\n   at Raven.Server.Documents.Indexes.MapIndexBase`2.UpdateIndexEntriesLucene(IndexItem indexItem, IEnumerable mapResults, Lazy`1 writer, TransactionOperationContext indexContext, IndexingStatsScope stats) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\MapIndexBase.cs:line 157\r\n   at Raven.Server.Documents.Indexes.MapIndexBase`2.HandleMap(IndexItem indexItem, IEnumerable mapResults, Lazy`1 writer, TransactionOperationContext indexContext, IndexingStatsScope stats) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\MapIndexBase.cs:line 59\r\n   at Raven.Server.Documents.Indexes.Workers.MapItems.Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext, Lazy`1 writeOperation, IndexingStatsScope stats, CancellationToken token) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\Workers\\MapItems.cs:line 143",
                    },
                    {
                        Timestamp: "2025-01-28T10:24:20.6682324Z",
                        Document: "orders/16-A",
                        Action: "Map",
                        Error: "Failed to execute mapping function on orders/16-A. Exception: System.DivideByZeroException: Attempted to divide by zero.\r\n   at CallSite.Target(Closure, CallSite, Object, Object)\r\n   at Raven.Server.Documents.Indexes.Static.Generated.Index_Orders_ByCompany.Map_0(IEnumerable`1 docs)+MoveNext()\r\n   at Raven.Server.Documents.Indexes.Static.TimeCountingEnumerable.Enumerator.MoveNext() in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\Static\\TimeCountingEnumerable.cs:line 47\r\n   at Raven.Server.Documents.Indexes.MapIndexBase`2.UpdateIndexEntriesLucene(IndexItem indexItem, IEnumerable mapResults, Lazy`1 writer, TransactionOperationContext indexContext, IndexingStatsScope stats) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\MapIndexBase.cs:line 157\r\n   at Raven.Server.Documents.Indexes.MapIndexBase`2.HandleMap(IndexItem indexItem, IEnumerable mapResults, Lazy`1 writer, TransactionOperationContext indexContext, IndexingStatsScope stats) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\MapIndexBase.cs:line 59\r\n   at Raven.Server.Documents.Indexes.Workers.MapItems.Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext, Lazy`1 writeOperation, IndexingStatsScope stats, CancellationToken token) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\Workers\\MapItems.cs:line 143",
                    },
                    {
                        Timestamp: "2025-01-28T10:24:20.6682674Z",
                        Document: "orders/17-A",
                        Action: "Map",
                        Error: "Failed to execute mapping function on orders/17-A. Exception: System.DivideByZeroException: Attempted to divide by zero.\r\n   at CallSite.Target(Closure, CallSite, Object, Object)\r\n   at Raven.Server.Documents.Indexes.Static.Generated.Index_Orders_ByCompany.Map_0(IEnumerable`1 docs)+MoveNext()\r\n   at Raven.Server.Documents.Indexes.Static.TimeCountingEnumerable.Enumerator.MoveNext() in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\Static\\TimeCountingEnumerable.cs:line 47\r\n   at Raven.Server.Documents.Indexes.MapIndexBase`2.UpdateIndexEntriesLucene(IndexItem indexItem, IEnumerable mapResults, Lazy`1 writer, TransactionOperationContext indexContext, IndexingStatsScope stats) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\MapIndexBase.cs:line 157\r\n   at Raven.Server.Documents.Indexes.MapIndexBase`2.HandleMap(IndexItem indexItem, IEnumerable mapResults, Lazy`1 writer, TransactionOperationContext indexContext, IndexingStatsScope stats) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\MapIndexBase.cs:line 59\r\n   at Raven.Server.Documents.Indexes.Workers.MapItems.Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext, Lazy`1 writeOperation, IndexingStatsScope stats, CancellationToken token) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\Workers\\MapItems.cs:line 143",
                    },
                    {
                        Timestamp: "2025-01-28T10:24:20.6683017Z",
                        Document: "orders/18-A",
                        Action: "Map",
                        Error: "Failed to execute mapping function on orders/18-A. Exception: System.DivideByZeroException: Attempted to divide by zero.\r\n   at CallSite.Target(Closure, CallSite, Object, Object)\r\n   at Raven.Server.Documents.Indexes.Static.Generated.Index_Orders_ByCompany.Map_0(IEnumerable`1 docs)+MoveNext()\r\n   at Raven.Server.Documents.Indexes.Static.TimeCountingEnumerable.Enumerator.MoveNext() in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\Static\\TimeCountingEnumerable.cs:line 47\r\n   at Raven.Server.Documents.Indexes.MapIndexBase`2.UpdateIndexEntriesLucene(IndexItem indexItem, IEnumerable mapResults, Lazy`1 writer, TransactionOperationContext indexContext, IndexingStatsScope stats) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\MapIndexBase.cs:line 157\r\n   at Raven.Server.Documents.Indexes.MapIndexBase`2.HandleMap(IndexItem indexItem, IEnumerable mapResults, Lazy`1 writer, TransactionOperationContext indexContext, IndexingStatsScope stats) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\MapIndexBase.cs:line 59\r\n   at Raven.Server.Documents.Indexes.Workers.MapItems.Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext, Lazy`1 writeOperation, IndexingStatsScope stats, CancellationToken token) in C:\\Users\\maksym.smolinski\\WebstormProjects\\ravendb-v6.2\\src\\Raven.Server\\Documents\\Indexes\\Workers\\MapItems.cs:line 143",
                    },
                ],
            },
            {
                Name: "Product/Rating",
                Errors: [],
            },
            {
                Name: "Companies/StockPrices/TradeVolumeByMonth",
                Errors: [],
            },
            {
                Name: "Products/ByUnitOnStock",
                Errors: [],
            },
            {
                Name: "Product/Search",
                Errors: [],
            },
        ];
    }
}
