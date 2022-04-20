import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import IndexProgress = Raven.Client.Documents.Indexes.IndexProgress;
import IndexUtils from "../../components/utils/IndexUtils";

const statsFixture = require("../fixtures/indexes_stats.json");
const progressFixture: { Results: IndexProgress[] } = require("../fixtures/indexes_progress.json");

export class IndexesStubs {
    static getSampleStats(): IndexStats[] {
        const stats: IndexStats[] = statsFixture.Results;

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
        return progressFixture.Results;
    }
}
