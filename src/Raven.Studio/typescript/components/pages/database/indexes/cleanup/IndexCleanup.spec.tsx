import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/testing-react";
import * as stories from "./IndexCleanup.stories";
import { fireEvent } from "@testing-library/react";

const { EmptyView, CleanupSuggestions, LicenseRestricted } = composeStories(stories);

describe("IndexCleanup", function () {
    it("can render empty view", async () => {
        const { screen } = rtlRender(<EmptyView />);

        await screen.findByText("No indexes to merge");
    });

    it("can render suggestions", async () => {
        const { screen } = rtlRender(<CleanupSuggestions />);

        expect(await screen.findByText("Review suggested merge")).toBeInTheDocument();
    });
    it("is license restricted", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        const infoHubButton = await screen.findByText("Info Hub");
        fireEvent.click(infoHubButton);

        const licensingText = await screen.findByText(/Licensing/i);
        expect(licensingText).toBeInTheDocument();
    });
});
