import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./ClientDatabaseConfiguration.stories";
import { composeStories } from "@storybook/react-webpack5";

const { WithGlobalConfiguration, WithoutGlobalConfiguration, LicenseRestricted } = composeStories(stories);

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

    it("goes back to the default identity parts separator when it is cleared", async () => {
        const { screen, user } = rtlRender(<WithoutGlobalConfiguration />);

        const separator = await screen.findByName("identityPartsSeparatorValue");
        await user.type(separator, "-");
        await user.clear(separator);

        expect(screen.queryByText(/exactly 1 character/)).not.toBeInTheDocument();
        expect(screen.queryByRole("button", { name: "Reset to default" })).not.toBeInTheDocument();
    });

    it("is license restricted", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByText(/Licensing/)).toBeInTheDocument();
    });
});
