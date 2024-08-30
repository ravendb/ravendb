import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/react";
import * as stories from "./StudioGlobalConfiguration.stories";

const { StudioConfiguration, LicenseRestricted } = composeStories(stories);

describe("StudioGlobalConfiguration", function () {
    it("can render", async () => {
        const { screen } = rtlRender(<StudioConfiguration />);
        expect(await screen.findByText("Collapse documents when opening")).toBeInTheDocument();
    });

    it("is license restricted", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByText(/Licensing/)).toBeInTheDocument();
    });
});
