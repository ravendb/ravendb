import React from "react";
import { composeStories } from "@storybook/react";
import * as stories from "./ConnectionStrings.stories";
import { rtlRender, RtlScreen, waitForElementToBeRemoved } from "test/rtlTestUtils";

const { DefaultConnectionStrings } = composeStories(stories);

const selectors = {
    edit: /edit connection string/i,
    delete: /delete connection string/i,
    addNew: /add new/i,
    emptyList: /no connection strings/i,
};

describe("ConnectionStrings", () => {
    async function waitForLoad(screen: RtlScreen) {
        await waitForElementToBeRemoved(screen.getByClassName("lazy-load"));
    }

    it("can render empty list", async () => {
        const { screen } = rtlRender(<DefaultConnectionStrings isEmpty />);
        await waitForLoad(screen);

        expect(screen.getByText(selectors.emptyList)).toBeInTheDocument();
    });

    it("can render connection strings", async () => {
        const { screen } = rtlRender(<DefaultConnectionStrings />);
        await waitForLoad(screen);

        expect(screen.queryByText(selectors.emptyList)).not.toBeInTheDocument();
        expect(screen.queryAllByClassName("rich-panel-name")).toHaveLength(6);
    });

    it("can render action buttons when has access database admin", async () => {
        const { screen } = rtlRender(<DefaultConnectionStrings databaseAccess="DatabaseAdmin" />);
        await waitForLoad(screen);

        // one on the top + one per connection string
        expect(screen.queryAllByRole("button", { name: selectors.addNew })).toHaveLength(7);

        // one per connection string
        expect(screen.queryAllByRole("button", { name: selectors.edit })).toHaveLength(6);
        expect(screen.queryAllByRole("button", { name: selectors.delete })).toHaveLength(6);
    });

    it("can hide action buttons when has access below database admin", async () => {
        const { screen } = rtlRender(<DefaultConnectionStrings databaseAccess="DatabaseRead" />);
        await waitForLoad(screen);

        expect(screen.queryAllByRole("button", { name: selectors.addNew })).toHaveLength(0);
        expect(screen.queryAllByRole("button", { name: selectors.edit })).toHaveLength(0);
        expect(screen.queryAllByRole("button", { name: selectors.delete })).toHaveLength(0);
    });

    it("can disable add button when no license", async () => {
        const { screen } = rtlRender(
            <DefaultConnectionStrings
                isEmpty
                hasElasticSearchEtl={false}
                hasSqlEtl={false}
                hasRavenEtl={false}
                hasOlapEtl={false}
                hasQueueEtl={false}
            />
        );
        await waitForLoad(screen);

        expect(screen.getByRole("button", { name: selectors.addNew })).toBeDisabled();
    });
});
