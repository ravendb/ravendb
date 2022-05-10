import { rtlRender } from "../../../../../test/rtlTestUtils";
import { DatabasesStubs } from "../../../../../test/stubs/DatabasesStubs";
import accessManager from "common/shell/accessManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { mockServices } from "../../../../../test/mocks/MockServices";
import { IndexesPage } from "./IndexesPage";
import React from "react";

function render() {
    const db = DatabasesStubs.shardedDatabase();
    return rtlRender(
        <div className="indexes content-margin no-transition absolute-fill">
            <IndexesPage database={db} />
        </div>
    );
}

describe("IndexesPage", function () {
    beforeEach(() => {
        accessManager.default.securityClearance("ClusterAdmin");
        clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");
    });

    it("can render empty view", async () => {
        const { indexesService } = mockServices;

        indexesService.withGetSampleStats([]);
        indexesService.withGetProgress([]);

        const { screen } = render();

        await screen.findByText(/No indexes have been created for this database/i);
    });

    it("can render", async () => {
        const { indexesService } = mockServices;

        indexesService.withGetSampleStats();
        indexesService.withGetProgress();

        const { screen } = render();

        await screen.findByText("Orders/ByCompany");
        await screen.findByText("ReplacementOf/Orders/ByCompany");
        const deleteButtons = await screen.findAllByTitle(/Delete the index/i);
        expect(deleteButtons.length).toBeGreaterThanOrEqual(1);
    });
});
