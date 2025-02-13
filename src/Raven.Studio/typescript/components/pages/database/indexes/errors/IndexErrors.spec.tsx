import { rtlRender } from "test/rtlTestUtils";
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
    title: "Index Errors",
    clearErrorsButtonLabel: "Clear Errors",
};

const totalErrorCount = IndexesStubs.getIndexesErrorCount().Results.reduce(
    (count, item) => count + item.Errors.reduce((sum, error) => sum + error.NumberOfErrors, 0),
    0
);

describe("IndexErrors", function () {
    it("renders a single non-sharded node without errors", async () => {
        const { screen } = rtlRender(
            <IndexErrorsStory hasErrors={false} databaseAccess="DatabaseAdmin" isSharded={false} />
        );

        expect(await screen.findByRole("heading", { name: textSelectors.title })).toBeInTheDocument();
        expect(await screen.findByText(textSelectors.nodePanelItemStatusBadge)).toBeInTheDocument();
        expect(await screen.findAllByClassName(classSelectors.nodePanel)).toHaveLength(1);
    });

    it("renders sharded nodes without errors", async () => {
        const { screen } = rtlRender(<IndexErrorsStory hasErrors={false} databaseAccess="DatabaseAdmin" isSharded />);

        expect(await screen.findByRole("heading", { name: textSelectors.title })).toBeInTheDocument();
        expect(await screen.findAllByClassName(classSelectors.nodePanel)).toHaveLength(6);
    });

    it("renders a single non-sharded node with errors and displays total count", async () => {
        const { screen } = rtlRender(<IndexErrorsStory hasErrors databaseAccess="DatabaseAdmin" isSharded={false} />);

        expect(await screen.findByRole("heading", { name: textSelectors.title })).toBeInTheDocument();
        expect(await screen.findByText(textSelectors.erroredNodePanelItemStatusBadge)).toBeInTheDocument();

        const totalErrorsElement = (
            await screen.findByText(textSelectors.erroredNodePanelTotalErrorCount)
        ).closest<HTMLElement>(classSelectors.erroredNodePanelTotalErrorCountContainer);

        expect(within(totalErrorsElement).getByText(`${totalErrorCount} errors`)).toBeInTheDocument();
    });

    it("does not show 'Clear errors' button for users with 'DatabaseRead' access", async () => {
        const { screen } = rtlRender(<IndexErrorsStory hasErrors databaseAccess="DatabaseRead" isSharded={false} />);

        expect(await screen.findByRole("heading", { name: textSelectors.title })).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: textSelectors.clearErrorsButtonLabel })).not.toBeInTheDocument();
    });

    it("renders a shard icon with isSharded is true", async () => {
        const { screen } = rtlRender(<IndexErrorsStory hasErrors databaseAccess="DatabaseRead" isSharded />);

        expect((await screen.findAllByClassName("icon-shard"))[0]).toBeInTheDocument();
    });

    it("does not render shard icon with isSharded is false", async () => {
        const { screen } = rtlRender(<IndexErrorsStory hasErrors databaseAccess="DatabaseRead" isSharded={false} />);

        expect(screen.queryByClassName("icon-shard")).not.toBeInTheDocument();
    });
});
