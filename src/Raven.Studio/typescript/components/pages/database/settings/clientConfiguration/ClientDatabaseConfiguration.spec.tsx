import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./ClientDatabaseConfiguration.stories";
import { composeStories } from "@storybook/react";

const { WithGlobalConfiguration, WithoutGlobalConfiguration } = composeStories(stories);

const serverConfigurationText = "Server Configuration";

describe("ClientDatabaseConfiguration", function () {
    it("can render with global config", async () => {
        const { screen } = rtlRender(<WithGlobalConfiguration />);

        expect(await screen.findByText(/Save/)).toBeInTheDocument();
        expect(screen.queryByText(serverConfigurationText)).toBeInTheDocument();
    });

    it("can render without global config", async () => {
        const { screen } = rtlRender(<WithoutGlobalConfiguration />);

        expect(await screen.findByText(/Save/)).toBeInTheDocument();
        expect(screen.queryByText(serverConfigurationText)).not.toBeInTheDocument();
    });
});
