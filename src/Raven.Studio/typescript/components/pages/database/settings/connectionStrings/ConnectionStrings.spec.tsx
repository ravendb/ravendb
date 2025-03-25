import React from "react";
import { composeStories } from "@storybook/react";
import * as stories from "./ConnectionStrings.stories";
import { rtlRender_WithWaitForLoad } from "test/rtlTestUtils";

const { DefaultConnectionStrings } = composeStories(stories);

const selectors = {
    edit: /edit connection string/i,
    delete: /delete connection string/i,
    addNew: /add new/i,
    emptyList: /no connection strings/i,
};

describe("ConnectionStrings", () => {
    it("can render empty list", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultConnectionStrings isEmpty />);

        expect(screen.getByText(selectors.emptyList)).toBeInTheDocument();
    });

    it("can render connection strings", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultConnectionStrings />);

        expect(screen.queryByText(selectors.emptyList)).not.toBeInTheDocument();
        expect(screen.queryAllByClassName("rich-panel-name")).toHaveLength(10);
    });

    it("can render action buttons when has access database admin", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultConnectionStrings databaseAccess="DatabaseAdmin" />);

        // one on the top + one per connection string
        expect(screen.queryAllByRole("button", { name: selectors.addNew })).toHaveLength(11);

        // one per connection string
        expect(screen.queryAllByRole("button", { name: selectors.edit })).toHaveLength(10);
        expect(screen.queryAllByRole("button", { name: selectors.delete })).toHaveLength(10);
    });

    it("can hide action buttons when has access below database admin", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultConnectionStrings databaseAccess="DatabaseRead" />);

        expect(screen.queryAllByRole("button", { name: selectors.addNew })).toHaveLength(0);
        expect(screen.queryAllByRole("button", { name: selectors.edit })).toHaveLength(0);
        expect(screen.queryAllByRole("button", { name: selectors.delete })).toHaveLength(0);
    });
});
