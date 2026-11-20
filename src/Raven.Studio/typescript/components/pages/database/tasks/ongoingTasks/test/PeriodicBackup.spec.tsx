import { composeStory } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/tasks/ongoingTasks/stories/PeriodicBackup.stories";
import { rtlRender } from "test/rtlTestUtils";
import { within } from "@testing-library/dom";
import React from "react";

const containerTestId = "backups";

describe("Periodic Backup", function () {
    it("can render enabled", async () => {
        const Story = composeStory(stories.Default, stories.default);

        const { screen } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));
        expect(await container.findByText(/Periodic Backup/)).toBeInTheDocument();
        expect(await container.findByText(/Enabled/)).toBeInTheDocument();
        expect(container.queryByText(/Disabled/)).not.toBeInTheDocument();

        expect(await container.findByText(/Destinations/)).toBeInTheDocument();
        expect(await container.findByText(/Last Full Backup/)).toBeInTheDocument();
        expect(await container.findByText(/Last Incremental Backup/)).toBeInTheDocument();
        expect(await container.findByText(/Next Estimated Backup/)).toBeInTheDocument();
        expect(await container.findByText(/Retention Policy/)).toBeInTheDocument();
        expect(container.queryByTitle(/Last backup failed/)).not.toBeInTheDocument();
    });

    it("shows the last backup error", async () => {
        const Story = composeStory(stories.LastBackupFailed, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        await fireClick(await container.findByTitle(/Last backup failed/));

        expect(await screen.findByRole("dialog")).toHaveTextContent("Unable to connect to the remote server");
    });
});
