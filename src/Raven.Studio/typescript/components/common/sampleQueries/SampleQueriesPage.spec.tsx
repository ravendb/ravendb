import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import * as stories from "./SampleQueriesPage.stories";
import { rtlRender } from "test/rtlTestUtils";

const { Default } = composeStories(stories);

const selectors = {
    updateScriptButton: /update script/i,
    resetButton: /reset/i,
    sampleScriptsTab: /sample scripts/i,
    methodsTab: /methods/i,
    scriptTitle: "Filter out an array item",
    scriptDescription: "Removes a specific line item (product ID 'products/1') from all order documents.",
    methodsSignatureHeader: "Methods signature",
    descriptionHeader: "Description",
    searchMethodsPlaceholder: "Search methods",
    methodCategory: "Document operations",
    methodSignature: "load(documentIdToLoad)",
};

describe("SampleQueriesPage", () => {
    it("renders action buttons", async () => {
        const { screen } = rtlRender(<Default />);

        expect(await screen.findByRole("button", { name: selectors.updateScriptButton })).toBeInTheDocument();
        expect(await screen.findByRole("button", { name: selectors.resetButton })).toBeInTheDocument();
    });

    it("shows sample scripts tab by default", async () => {
        const { screen } = rtlRender(<Default />);

        expect(await screen.findByRole("tab", { name: selectors.sampleScriptsTab })).toBeInTheDocument();
    });

    it("shows script title and description in scripts tab", async () => {
        const { screen } = rtlRender(<Default />);

        expect(await screen.findByText(selectors.scriptTitle)).toBeInTheDocument();
        expect(await screen.findByText(selectors.scriptDescription)).toBeInTheDocument();
    });

    it("clicking Methods tab shows table column headers", async () => {
        const { screen, fireClick } = rtlRender(<Default />);

        const methodsTab = await screen.findByRole("tab", { name: selectors.methodsTab });
        await fireClick(methodsTab);

        expect(await screen.findByText(selectors.methodsSignatureHeader)).toBeInTheDocument();
        expect(await screen.findByText(selectors.descriptionHeader)).toBeInTheDocument();
    });

    it("clicking Methods tab shows Search methods input", async () => {
        const { screen, fireClick } = rtlRender(<Default />);

        const methodsTab = await screen.findByRole("tab", { name: selectors.methodsTab });
        await fireClick(methodsTab);

        expect(await screen.findByPlaceholderText(selectors.searchMethodsPlaceholder)).toBeInTheDocument();
    });

    it("clicking Methods tab shows method category and signature", async () => {
        const { screen, fireClick } = rtlRender(<Default />);

        const methodsTab = await screen.findByRole("tab", { name: selectors.methodsTab });
        await fireClick(methodsTab);

        expect(await screen.findByText(selectors.methodCategory)).toBeInTheDocument();
        expect(await screen.findByText(selectors.methodSignature)).toBeInTheDocument();
    });
});
