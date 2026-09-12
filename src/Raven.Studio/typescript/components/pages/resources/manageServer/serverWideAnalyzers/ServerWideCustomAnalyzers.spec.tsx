import { rtlRender } from "test/rtlTestUtils";
import * as Stories from "./ServerWideCustomAnalyzers.stories";
import { composeStories } from "@storybook/react-webpack5";
import React from "react";

const { ServerWideCustomAnalyzersStory } = composeStories(Stories);

const selectors = {
    licenseBadgeTestId: "license-restricted-badge",
    saveButton: /Save changes/,
    emptyList: /No server-wide custom analyzers have been defined/,
};

describe("ServerWideCustomAnalyzers", () => {
    it("can render when feature is not in license", async () => {
        const { screen, waitForLoad } = rtlRender(
            <ServerWideCustomAnalyzersStory hasServerWideCustomAnalyzers={false} />
        );
        await waitForLoad();

        expect(screen.queryByTestId(selectors.licenseBadgeTestId)).toBeInTheDocument();
        expect(screen.queryByText(selectors.emptyList)).toBeInTheDocument();
    });

    it("can render when feature is in license", async () => {
        const { screen, waitForLoad, fireClick } = rtlRender(
            <ServerWideCustomAnalyzersStory hasServerWideCustomAnalyzers />
        );
        await waitForLoad();

        expect(screen.queryByTestId(selectors.licenseBadgeTestId)).not.toBeInTheDocument();

        await fireClick(screen.getAllByClassName("icon-edit")[0]);
        expect(screen.queryByText(selectors.saveButton)).toBeInTheDocument();
    });
});
