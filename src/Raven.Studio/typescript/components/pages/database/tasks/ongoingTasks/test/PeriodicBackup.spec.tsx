import { composeStory } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/tasks/ongoingTasks/stories/PeriodicBackup.stories";
import { rtlRender } from "test/rtlTestUtils";
import { within } from "@testing-library/dom";
import React from "react";

const containerTestId = "backups";

describe("Periodic Backup", function () {
    it("can render enabled", async () => {
        const Story = composeStory(stories.Default, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));
        expect(await container.findByText(/Periodic Backup/)).toBeInTheDocument();
        expect(await container.findByText(/Enabled/)).toBeInTheDocument();
        expect(container.queryByText(/Disabled/)).not.toBeInTheDocument();

        const detailsBtn = await container.findByTitle(/Click for details/);

        await fireClick(detailsBtn);

        expect(await container.findByText(/Destinations/)).toBeInTheDocument();
        expect(await container.findByText(/Last Full Backup/)).toBeInTheDocument();
        expect(await container.findByText(/Last Incremental Backup/)).toBeInTheDocument();
        expect(await container.findByText(/Next Estimated Backup/)).toBeInTheDocument();
        expect(await container.findByText(/Retention Policy/)).toBeInTheDocument();
    });
});
