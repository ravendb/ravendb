import { rtlRender } from "test/rtlTestUtils";
import * as Stories from "./ServerWideCustomSorters.stories";
import { composeStories } from "@storybook/react";
import React from "react";

const { ServerWideCustomSortersStory } = composeStories(Stories);

const selectors = {
    licenseBadge: /Professional +/,
    saveButton: /Save changes/,
    emptyList: /No server-wide custom sorters have been defined/,
};

describe("ServerWideCustomSorters", () => {
    it("can render when feature is not in license", async () => {
        const { screen, waitForLoad } = rtlRender(<ServerWideCustomSortersStory hasServerWideCustomSorters={false} />);
        await waitForLoad();

        expect(screen.queryByText(selectors.licenseBadge)).toBeInTheDocument();
        expect(screen.queryByText(selectors.emptyList)).toBeInTheDocument();
    });

    it("can render when feature is in license", async () => {
        const { screen, waitForLoad, fireClick } = rtlRender(
            <ServerWideCustomSortersStory hasServerWideCustomSorters />
        );
        await waitForLoad();

        expect(screen.queryByText(selectors.licenseBadge)).not.toBeInTheDocument();

        await fireClick(screen.getAllByClassName("icon-edit")[0]);
        expect(screen.queryByText(selectors.saveButton)).toBeInTheDocument();
    });
});
