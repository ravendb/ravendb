import { rtlRender_WithWaitForLoad, waitFor } from "test/rtlTestUtils";
import * as Stories from "./ServerWideTasks.stories";
import { composeStories } from "@storybook/react-webpack5";
import React from "react";
import router from "plugins/router";
import appUrl from "common/appUrl";
import { mockServices } from "test/mocks/services/MockServices";

const { ServerWideTasksStory } = composeStories(Stories);

const selectors = {
    addButton: /Add a Server-Wide Task/,
    backupTaskName: /BackupTask/,
    replicationTaskName: /ExternalReplicationTask/,
    replicationSection: /External replication/,
    nameFilterPlaceholder: /e.g. BackupTask/,
    noMatchingTasks: /No tasks match your filter criteria/,
};

describe("ServerWideTasks", () => {
    it("redirects to the add view when there are no tasks", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<ServerWideTasksStory isEmpty />);

        // With no tasks the page redirects to the add view instead of rendering the list
        expect(screen.queryByText(selectors.addButton)).not.toBeInTheDocument();
        expect(screen.queryByText(selectors.backupTaskName)).not.toBeInTheDocument();
    });

    it("can render tasks grouped by type", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<ServerWideTasksStory />);

        expect(screen.queryByText(selectors.backupTaskName)).toBeInTheDocument();
        expect(screen.queryByText(selectors.replicationTaskName)).toBeInTheDocument();
        expect(screen.queryAllByText(selectors.replicationSection).length).toBeGreaterThan(0);
        expect(screen.queryByText(selectors.addButton)).toBeInTheDocument();
    });

    it("can select a task and open the bulk delete confirmation", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<ServerWideTasksStory />);

        const backupPanel = screen.getByText(selectors.backupTaskName).closest(".rich-panel-item");
        await fireClick(backupPanel.querySelector<HTMLInputElement>("input[type=checkbox]"));

        await fireClick(screen.getByText("Delete"));

        expect(await screen.findByText(/You're about to/)).toBeInTheDocument();
    });

    it("redirects to the add view after deleting the last tasks", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<ServerWideTasksStory />);

        const backupPanel = screen.getByText(selectors.backupTaskName).closest(".rich-panel-item");
        await fireClick(backupPanel.querySelector<HTMLInputElement>("input[type=checkbox]"));

        await fireClick(screen.getByText("Delete"));
        expect(await screen.findByText(/You're about to/)).toBeInTheDocument();

        // After the deletion the server returns an empty task list
        mockServices.manageServerService.withServerWideTasks({ Tasks: [] });

        const deleteButtons = screen.getAllByText("Delete");
        await fireClick(deleteButtons[deleteButtons.length - 1]);

        await waitFor(() => {
            expect(router.navigate).toHaveBeenCalledWith(appUrl.forAddServerWideTask(), {
                replace: true,
                trigger: true,
            });
        });
    });

    it("can filter tasks by name", async () => {
        const { screen, fillInput } = await rtlRender_WithWaitForLoad(<ServerWideTasksStory />);

        const nameFilterInput = screen.getByPlaceholderText(selectors.nameFilterPlaceholder);

        await fillInput(nameFilterInput, "BackupTask");

        expect(screen.queryByText(selectors.backupTaskName)).toBeInTheDocument();
        expect(screen.queryByText(selectors.replicationTaskName)).not.toBeInTheDocument();
        expect(screen.queryByText(selectors.noMatchingTasks)).not.toBeInTheDocument();

        await fillInput(nameFilterInput, "nothing-matches");

        expect(screen.queryByText(selectors.noMatchingTasks)).toBeInTheDocument();
        expect(screen.queryByText(selectors.backupTaskName)).not.toBeInTheDocument();
    });
});
