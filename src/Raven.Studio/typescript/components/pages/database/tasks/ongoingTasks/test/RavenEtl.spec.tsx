import { composeStory } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/tasks/ongoingTasks/stories/RavenEtl.stories";
import { act, rtlRender, waitFor } from "test/rtlTestUtils";
import { within } from "@testing-library/dom";
import React from "react";
import { selectors } from "./selectors";
import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import TasksService from "components/services/TasksService";

const containerTestId = "etls";

describe("RavenETL", function () {
    it("can render disabled and not completed", async () => {
        const Story = composeStory(stories.Disabled, stories.default);

        const { screen, fireClick } = rtlRender(<Story disabled completed={false} />);
        const container = within(await screen.findByTestId(containerTestId));

        expect(await container.findByText(/RavenDB ETL/)).toBeInTheDocument();
        expect(await container.findByText(/Disabled/, { selector: "button" })).toBeInTheDocument();
        expect(container.queryByText(/Enabled/)).not.toBeInTheDocument();

        const detailsBtn = await container.findByTitle(/Click for details/);

        await fireClick(detailsBtn);

        expect(await container.findByText(/Topology Discovery URLs/)).toBeInTheDocument();

        //wait for progress
        await container.findAllByText(/Disabled/i);
    });

    it("can render completed", async () => {
        const Story = composeStory(stories.Default, stories.default);

        const { screen, fireClick } = rtlRender(<Story completed />);
        const container = within(await screen.findByTestId(containerTestId));
        const detailsBtn = await container.findByTitle(/Click for details/);
        await fireClick(detailsBtn);

        //wait for progress
        await container.findAllByText(/Up to date/i);
    });

    it("can render enabled and not completed", async () => {
        const Story = composeStory(stories.Default, stories.default);

        const { screen, fireClick } = rtlRender(<Story completed={false} disabled={false} />);
        const container = within(await screen.findByTestId(containerTestId));
        const detailsBtn = await container.findByTitle(/Click for details/);
        await fireClick(detailsBtn);

        //wait for progress
        await container.findAllByText("Running");
    });

    it("can notify about empty script", async () => {
        const Story = composeStory(stories.EmptyScript, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        const detailsBtn = await container.findByTitle(/Click for details/);
        await fireClick(detailsBtn);

        //wait for progress
        await container.findAllByText(/Up to date/i);

        expect(await container.findByText(selectors.emptyScriptText)).toBeInTheDocument();
    });

    it("keeps the calculated exact progress while the tooltip is open", async () => {
        jest.useFakeTimers({ advanceTimers: true });
        const getEtlProgress = jest.mocked(mockServices.tasksService.mock.getEtlProgress);

        try {
            const Story = composeStory(stories.Default, stories.default);

            const { screen, fireClick, user } = rtlRender(<Story completed={false} disabled={false} />);
            const container = within(await screen.findByTestId(containerTestId));
            await fireClick(await container.findByTitle(/Click for details/));
            const [etlItem] = await container.findAllByText("Running");

            const estimatedProgress = TasksStubs.getEtlTasksProgress();
            const exactProgress = TasksStubs.getEtlTasksProgress();
            exactProgress.Results.forEach((x) => x.ProcessesProgress.forEach((p) => (p.Estimated = false)));
            getEtlProgress.mockImplementation(async (...args: Parameters<TasksService["getEtlProgress"]>) => {
                const [, , , , exact] = args;
                return exact ? exactProgress : estimatedProgress;
            });

            await user.hover(etlItem);
            expect(await screen.findByText("(~)")).toBeInTheDocument();

            await fireClick(await screen.findByText("calculate exact counts"));

            const taskName = TasksStubs.getRavenEtlProgress().TaskName;
            expect(getEtlProgress).toHaveBeenLastCalledWith(
                expect.any(String),
                expect.anything(),
                true,
                taskName,
                true
            );
            await waitFor(() => expect(screen.queryByText("(~)")).not.toBeInTheDocument());

            act(() => {
                jest.advanceTimersByTime(10_000);
            });
            await waitFor(() =>
                expect(getEtlProgress).toHaveBeenLastCalledWith(expect.any(String), expect.anything(), false)
            );
            expect(screen.queryByText("(~)")).not.toBeInTheDocument();

            await user.unhover(etlItem);
            await waitFor(() => expect(screen.queryByText(/Script #1/)).not.toBeInTheDocument());

            await user.hover(etlItem);
            expect(await screen.findByText("(~)")).toBeInTheDocument();
        } finally {
            getEtlProgress.mockReset();
            jest.useRealTimers();
        }
    });
});
