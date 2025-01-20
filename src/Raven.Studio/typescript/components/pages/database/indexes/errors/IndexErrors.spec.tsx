import { rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/react";
import * as stories from "./IndexErrors.stories";
import { within } from "@testing-library/dom";
import { IndexesStubs } from "test/stubs/IndexesStubs";

const { IndexErrorsStory } = composeStories(stories);

const classSelectors = {
    nodePanel: "rich-panel-item",
    erroredNodePanelTotalErrorCountContainer: ".detail-item-content",
};

const textSelectors = {
    erroredNodePanelItemStatusBadge: "Errors",
    nodePanelItemStatusBadge: "OK",
    erroredNodePanelTotalErrorCount: "Total count",
};

const totalErrorCount = IndexesStubs.getIndexesErrorCount().Results.reduce(
    (count, item) => count + item.Errors.reduce((sum, error) => sum + error.NumberOfErrors, 0),
    0
);

describe("IndexErrors", function () {
    it("renders a single non-sharded node without errors", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors={false} databaseAccess="DatabaseAdmin" isSharded={false} />
        );

        expect(screen.getByRole("heading", { name: "Index Errors" })).toBeInTheDocument();
        expect(screen.getByText(textSelectors.nodePanelItemStatusBadge)).toBeInTheDocument();
        expect(screen.getAllByClassName(classSelectors.nodePanel)).toHaveLength(1);
    });

    it("renders sharded nodes without errors", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors={false} databaseAccess="DatabaseAdmin" isSharded />
        );

        expect(screen.getByRole("heading", { name: "Index Errors" })).toBeInTheDocument();
        expect(screen.getAllByClassName(classSelectors.nodePanel)).toHaveLength(6);
    });

    it("renders a single non-sharded node with errors and displays total count", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors databaseAccess="DatabaseAdmin" isSharded={false} />
        );

        expect(screen.getByRole("heading", { name: "Index Errors" })).toBeInTheDocument();
        expect(screen.getByText(textSelectors.erroredNodePanelItemStatusBadge)).toBeInTheDocument();

        const totalErrorsElement = screen
            .getByText(textSelectors.erroredNodePanelTotalErrorCount)
            .closest<HTMLElement>(classSelectors.erroredNodePanelTotalErrorCountContainer);

        expect(within(totalErrorsElement).getByText(`${totalErrorCount} errors`)).toBeInTheDocument();
    });

    it("renders sharded nodes with errors and displays total count for each", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors databaseAccess="DatabaseAdmin" isSharded />
        );

        const totalErrorsElements = screen
            .getAllByText(textSelectors.erroredNodePanelTotalErrorCount)
            .map((x) => x.closest<HTMLElement>(classSelectors.erroredNodePanelTotalErrorCountContainer));

        for (const totalErrorsElement of totalErrorsElements) {
            expect(within(totalErrorsElement).getByText(`${totalErrorCount} errors`)).toBeInTheDocument();
        }
        expect(screen.getByRole("heading", { name: "Index Errors" })).toBeInTheDocument();
        expect(screen.getAllByClassName(classSelectors.nodePanel)).toHaveLength(6);
    });

    it("shows 'Clear errors' button for users with 'DatabaseReadWrite' access", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors databaseAccess="DatabaseReadWrite" isSharded={false} />
        );

        expect(screen.getByRole("button", { name: "Clear errors" })).toBeInTheDocument();
    });

    it("does not show 'Clear errors' button for users with 'DatabaseRead' access", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors databaseAccess="DatabaseRead" isSharded={false} />
        );

        expect(screen.queryByRole("button", { name: "Clear errors" })).not.toBeInTheDocument();
    });

    it("renders a shard icon with isSharded is true", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors databaseAccess="DatabaseRead" isSharded />
        );

        expect(screen.getAllByClassName("icon-shard")[0]).toBeInTheDocument();
    });

    it("does not render shard icon with isSharded is false", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors databaseAccess="DatabaseRead" isSharded={false} />
        );

        expect(screen.queryByClassName("icon-shard")).not.toBeInTheDocument();
    });
});
