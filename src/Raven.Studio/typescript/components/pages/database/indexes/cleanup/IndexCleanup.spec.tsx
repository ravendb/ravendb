import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/testing-react";
import * as stories from "./IndexCleanup.stories";

const { EmptyView, CleanupSuggestions } = composeStories(stories);

describe("IndexCleanup", function () {
    it("can render empty view", async () => {
        const { screen } = rtlRender(<EmptyView />);

        await screen.findByText("No indexes to merge");
    });

    it("can render suggestions", async () => {
        const { screen } = rtlRender(<CleanupSuggestions />);

        expect(await screen.findByText("Review suggested merge")).toBeInTheDocument();
    });
});
