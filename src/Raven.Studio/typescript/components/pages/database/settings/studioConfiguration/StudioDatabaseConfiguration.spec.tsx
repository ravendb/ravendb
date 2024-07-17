import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/react";
import * as stories from "./StudioDatabaseConfiguration.stories";

const { StudioConfiguration, LicenseRestricted } = composeStories(stories);

describe("StudioDatabaseConfiguration", function () {
    it("can render", async () => {
        const { screen } = rtlRender(<StudioConfiguration />);
        expect(await screen.findByText("Go to Server-Wide Studio Configuration View")).toBeInTheDocument();
    });

    it("is license restricted", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByText(/Licensing/)).toBeInTheDocument();
    });
});
