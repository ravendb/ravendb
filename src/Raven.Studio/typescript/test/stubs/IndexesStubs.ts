import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import IndexProgress = Raven.Client.Documents.Indexes.IndexProgress;
import IndexUtils from "../../components/utils/IndexUtils";
import CollectionStats = Raven.Client.Documents.Indexes.IndexProgress.CollectionStats;

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
            },
            SecondCollection: {
                LastProcessedItemEtag: 5,
                LastProcessedTombstoneEtag: 5,
                NumberOfItemsToProcess: 500,
                NumberOfTombstonesToProcess: 300,
                TotalNumberOfItems: 3000,
                TotalNumberOfTombstones: 5000,
            },
        };
    }

    static getDisabledIndex(): [IndexStats, IndexProgress] {
        const stats = IndexesStubs.getGenericStats();
        stats.Name = "DisabledIndex";
        stats.IsStale = true;
        stats.State = "Disabled";
        stats.Status = "Disabled";

        const progress = IndexesStubs.getGenericProgress();
        progress.IsStale = true;
        progress.Name = stats.Name;
        progress.Collections = IndexesStubs.collectionStats();
        return [stats, progress];
    }

    static getPausedIndex(): [IndexStats, IndexProgress] {
        const stats = IndexesStubs.getGenericStats();
        stats.Name = "PausedIndex";
        stats.IsStale = true;
        stats.State = "Normal";
        stats.Status = "Paused";

        const progress = IndexesStubs.getGenericProgress();
        progress.IsStale = true;
        progress.Name = stats.Name;
        progress.Collections = IndexesStubs.collectionStats();
        return [stats, progress];
    }

    static getFaultyIndex(): [IndexStats, IndexProgress] {
        const stats = IndexesStubs.getGenericStats();
        stats.Name = "FaultyIndex";
        stats.IsStale = true;
        stats.Type = "Faulty";
        return [stats, null];
    }

    static getErroredIndex(): [IndexStats, IndexProgress] {
        const stats = IndexesStubs.getGenericStats();
        stats.Name = "ErroredIndex";
        stats.IsStale = true;
        stats.State = "Error";
        return [stats, null];
    }
}
