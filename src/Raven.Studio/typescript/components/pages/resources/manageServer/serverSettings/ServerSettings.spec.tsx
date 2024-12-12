import { composeStories } from "@storybook/react";
import * as stories from "./ServerSettings.stories";
import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { expect } from "@storybook/test";

const { ServerSettingsStory } = composeStories(stories);

describe("ServerSettings", () => {
    it(`user have access to server settings and can see the view when clearance is ClusterAdmin or above`, () => {
        const { screen } = rtlRender(<ServerSettingsStory securityClearance="ClusterAdmin" />);

        const ServerSettingsView = screen.queryByText("Server Settings");
        expect(ServerSettingsView).toBeInTheDocument();
    });

    it(`user doesn't have access to server settings and can see insufficient access when clearance is below ClusterAdmin`, () => {
        const { screen } = rtlRender(<ServerSettingsStory securityClearance="ValidUser" />);

        const ServerSettingsView = screen.queryByText("Server Settings");
        const insufficientAccessView = screen.queryByText("You are not authorized to view this page");
        expect(ServerSettingsView).not.toBeInTheDocument();
        expect(insufficientAccessView).toBeInTheDocument();
    });
});
