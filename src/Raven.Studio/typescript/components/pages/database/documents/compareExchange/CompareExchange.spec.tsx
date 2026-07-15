import { composeStories } from "@storybook/react-webpack5";
import * as stories from "./CompareExchange.stories";
import { rtlRender } from "test/rtlTestUtils";
import React from "react";

const { CompareExchangeStory } = composeStories(stories);

const selectors = {
    addNewItemBtn: "Add new item",
    deleteBtn: /Delete/,
    keyColumnHeader: "Compare Exchange Key",
};

describe("CompareExchange", () => {
    // virtualized rows don't render in jsdom (virtualizer gets no real layout),
    // so assertions stick to headers/buttons; row-level behavior is covered in Storybook/manual checks
    it("renders table header and action buttons for read-write access", async () => {
        const { screen } = rtlRender(<CompareExchangeStory databaseAccess="DatabaseReadWrite" />);

        expect(await screen.findByText(selectors.keyColumnHeader)).toBeInTheDocument();
        expect(screen.getByRole("button", { name: selectors.addNewItemBtn })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: selectors.deleteBtn })).toBeInTheDocument();
    });

    it("hides action buttons and checkboxes for read-only access", async () => {
        const { screen } = rtlRender(<CompareExchangeStory databaseAccess="DatabaseRead" />);

        expect(await screen.findByText(selectors.keyColumnHeader)).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: selectors.addNewItemBtn })).not.toBeInTheDocument();
        expect(screen.queryByRole("button", { name: selectors.deleteBtn })).not.toBeInTheDocument();
        expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
    });

    it("delete button is disabled when nothing is selected", async () => {
        const { screen } = rtlRender(<CompareExchangeStory databaseAccess="DatabaseReadWrite" />);

        expect(await screen.findByText(selectors.keyColumnHeader)).toBeInTheDocument();
        expect(screen.getByRole("button", { name: selectors.deleteBtn })).toBeDisabled();
    });
});
