import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./I18nDemo.stories";

const { SideBySide } = composeStories(stories);

describe("I18nDemo", () => {
    it("renders the same view in English and Polish", async () => {
        const { screen } = rtlRender(<SideBySide />);

        expect(await screen.findByText("Enable Document Refresh")).toBeInTheDocument();
        expect(await screen.findByText("Włącz odświeżanie dokumentów")).toBeInTheDocument();
    });
});
