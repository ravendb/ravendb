import { rtlRender } from "../../../../../test/rtlTestUtils";
import { DatabasesStubs } from "../../../../../test/stubs/DatabasesStubs";
import { IndexesPage } from "./IndexesPage";
import React from "react";
import { composeStories } from "@storybook/testing-react";

import * as stories from "./IndexesPage.stories";
const { EmptyView, SampleDataCluster } = composeStories(stories);

function render() {
    const db = DatabasesStubs.shardedDatabase();
    return rtlRender(
        <div className="indexes content-margin no-transition">
            <IndexesPage database={db} />
        </div>
    );
}

describe("IndexesPage", function () {
    it("can render empty view", async () => {
        const { screen } = rtlRender(<EmptyView />);

        await screen.findByText(/No indexes have been created for this database/i);
    });

    it("can render", async () => {
        const { screen } = rtlRender(<SampleDataCluster />);

        await screen.findByText("Orders/ByCompany");
        await screen.findByText("ReplacementOf/Orders/ByCompany");
        const deleteButtons = await screen.findAllByTitle(/Delete the index/i);
        expect(deleteButtons.length).toBeGreaterThanOrEqual(1);
    });
});
