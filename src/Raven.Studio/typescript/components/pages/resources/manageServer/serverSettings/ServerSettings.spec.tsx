import { composeStories } from "@storybook/react";
import * as stories from "./ServerSettings.stories";
import { rtlRender, rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import React from "react";

const { ServerSettingsStory } = composeStories(stories);

describe("ServerSettings", () => {
    it(`user have access to server settings and can see the view when clearance is ClusterAdmin or above`, async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<ServerSettingsStory securityClearance="ClusterAdmin" />);

        const serverSettingsHeadingText = await screen.findByRole("heading", { name: /Server Settings/ });
        expect(serverSettingsHeadingText).toBeInTheDocument();
    });

    it(`user doesn't have access to server settings and can see insufficient access when clearance is below ClusterAdmin`, async () => {
        const { screen } = rtlRender(<ServerSettingsStory securityClearance="ValidUser" />);

        const serverSettingsHeadingText = screen.queryByText("Server Settings");
        const insufficientAccessText = await screen.findByText("You are not authorized to view this page");
        expect(serverSettingsHeadingText).not.toBeInTheDocument();
        expect(insufficientAccessText).toBeInTheDocument();
    });
});
