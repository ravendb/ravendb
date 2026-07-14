import { rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import * as Stories from "./ServerWideTasks.stories";
import { composeStories } from "@storybook/react-webpack5";
import React from "react";

const { ServerWideTasksStory } = composeStories(Stories);

const selectors = {
    emptyState: /No server-wide tasks configured yet/,
    createButton: /Create Server-Wide Task/,
    addButton: /Add a Server-Wide Task/,
    backupTaskName: /BackupTask/,
    replicationTaskName: /ExternalReplicationTask/,
    replicationSection: /External replication/,
    nameFilterPlaceholder: /e.g. BackupTask/,
    noMatchingTasks: /No tasks match your filter criteria/,
};

describe("ServerWideTasks", () => {
    it("can render empty state", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<ServerWideTasksStory isEmpty />);

        expect(screen.queryByText(selectors.emptyState)).toBeInTheDocument();
        expect(screen.queryByText(selectors.createButton)).toBeInTheDocument();
        expect(screen.queryByText(selectors.addButton)).not.toBeInTheDocument();
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

    it("can filter tasks by name", async () => {
        const { screen, fillInput } = await rtlRender_WithWaitForLoad(<ServerWideTasksStory />);

        await fillInput(screen.getByPlaceholderText(selectors.nameFilterPlaceholder), "nothing-matches");

        expect(screen.queryByText(selectors.noMatchingTasks)).toBeInTheDocument();
        expect(screen.queryByText(selectors.backupTaskName)).not.toBeInTheDocument();
    });
});
