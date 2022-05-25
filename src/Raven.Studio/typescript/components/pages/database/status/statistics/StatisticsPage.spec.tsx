import accessManager from "common/shell/accessManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { DatabasesStubs } from "../../../../../test/stubs/DatabasesStubs";
import { rtlRender } from "../../../../../test/rtlTestUtils";
import React from "react";
import { StatisticsPage } from "./StatisticsPage";
import { mockServices } from "../../../../../test/mocks/MockServices";

function render() {
    const db = DatabasesStubs.shardedDatabase();
    return rtlRender(
        <div>
            <StatisticsPage database={db} />
        </div>
    );
}

const selectors = {
    detailedStatsHeader: /Detailed Database Stats/i,
    documentsCount: /Documents Count/,
    showDetails: /show details/i,
};

describe("StatisticsPage", function () {
    beforeEach(() => {
        accessManager.default.securityClearance("ClusterAdmin");
        clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");
    });

    it("can render stats w/o details", async () => {
        const { databasesService } = mockServices;

        databasesService.withEssentialStats();

        const { screen } = render();

        expect(await screen.findByText(selectors.documentsCount)).toBeInTheDocument();
    });

    it("can render stats w/ details", async () => {
        const { databasesService } = mockServices;

        databasesService.withEssentialStats();
        databasesService.withDetailedStats();

        const { screen, fireClick } = render();

        expect(await screen.findByText(selectors.documentsCount)).toBeInTheDocument();

        await fireClick(screen.queryByText(selectors.showDetails));

        expect(await screen.findByText(selectors.detailedStatsHeader)).toBeInTheDocument();
    });
});
