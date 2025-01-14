import { composeStories } from "@storybook/react";
import * as stories from "./ServerSettings.stories";
import { rtlRender, rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import React from "react";

const { ServerSettingsStory } = composeStories(stories);

describe("ServerSettings", () => {
    it(`user have access to server settings and can see the view when clearance is ClusterAdmin or above`, async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<ServerSettingsStory securityClearance="ClusterAdmin" />);

        const serverSettingsElements = await screen.findAllByText("Server Settings");
        expect(serverSettingsElements).toHaveLength(2);
    });

    it(`user doesn't have access to server settings and can see insufficient access when clearance is below ClusterAdmin`, async () => {
        const { screen } = rtlRender(<ServerSettingsStory securityClearance="ValidUser" />);

        const ServerSettingsView = screen.queryByText("Server Settings");
        const insufficientAccessView = await screen.findByText("You are not authorized to view this page");
        expect(ServerSettingsView).not.toBeInTheDocument();
        expect(insufficientAccessView).toBeInTheDocument();
    });
});
