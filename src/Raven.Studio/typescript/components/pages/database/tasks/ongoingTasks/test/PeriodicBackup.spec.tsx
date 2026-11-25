import { composeStory } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/tasks/ongoingTasks/stories/PeriodicBackup.stories";
import { act, rtlRender } from "test/rtlTestUtils";
import { within } from "@testing-library/dom";
import React from "react";
import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";

const containerTestId = "backups";
const RELOAD_INTERVAL_IN_MS = 10_000;

function withBackupScheduledAt(nextBackupDateTime: string) {
    mockServices.tasksService.withGetTasks((x) => {
        const task = TasksStubs.getPeriodicBackupListItem();
        task.LastFullBackup = "2026-11-18T02:00:00.000Z";
        task.LastIncrementalBackup = "2026-11-22T02:00:00.000Z";
        task.NextBackup.DateTime = nextBackupDateTime;
        x.OngoingTasks = [task];
        x.PullReplications = [];
        x.SubscriptionsCount = 0;
    });
}

describe("Periodic Backup", function () {
    afterEach(() => {
        jest.useRealTimers();
    });

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

        const errorDialog = await screen.findByRole("dialog");
        expect(errorDialog).toHaveTextContent("Last backup failed on");
        expect(errorDialog).toHaveTextContent("Unable to connect to the remote server");
    });

    it("shows the error of a run that failed after the view was opened", async () => {
        jest.useFakeTimers();
        const Story = composeStory(stories.Default, stories.default);

        const { screen } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        withBackupScheduledAt("2026-11-25T02:00:00.000Z");
        await act(() => jest.advanceTimersByTimeAsync(RELOAD_INTERVAL_IN_MS));
        expect(container.queryByTitle(/Last backup failed/)).not.toBeInTheDocument();

        mockServices.tasksService.withGetPeriodicBackupStatus([TasksStubs.getFailedPeriodicBackupStatus()]);
        withBackupScheduledAt("2026-11-26T02:00:00.000Z");
        await act(() => jest.advanceTimersByTimeAsync(RELOAD_INTERVAL_IN_MS));

        expect(await container.findByTitle(/Last backup failed/)).toBeInTheDocument();
    });

    it("keeps the last backup error while the view refreshes", async () => {
        jest.useFakeTimers();
        const Story = composeStory(stories.LastBackupFailed, stories.default);

        const { screen } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));
        await container.findByTitle(/Last backup failed/);

        const getPeriodicBackupStatus = jest.mocked(mockServices.tasksService.mock.getPeriodicBackupStatus);
        getPeriodicBackupStatus.mockClear();
        getPeriodicBackupStatus.mockImplementation(() => new Promise(() => {}));
        withBackupScheduledAt("2026-11-26T02:00:00.000Z");
        await act(() => jest.advanceTimersByTimeAsync(RELOAD_INTERVAL_IN_MS));

        expect(getPeriodicBackupStatus).toHaveBeenCalled();
        expect(container.getByTitle(/Last backup failed/)).toBeInTheDocument();
    });
});
