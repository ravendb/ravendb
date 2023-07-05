import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/testing-react";
import * as stories from "./StudioGlobalConfiguration.stories";

const { StudioConfiguration } = composeStories(stories);

describe("StudioGlobalConfiguration", function () {
    it("can render", async () => {
        const { screen } = rtlRender(<StudioConfiguration />);
        expect(await screen.findByText("Collapse documents when opening")).toBeInTheDocument();
    });
});
