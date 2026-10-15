import { rtlRender, waitFor, within } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/react-webpack5";

import * as stories from "./ManageDatabaseGroupPage.stories";
import { mockHooks } from "test/mocks/hooks/MockHooks";
import { mockServices } from "test/mocks/services/MockServices";

const {
    Cluster,
    PreventDeleteIgnore,
    ClusterWithDeletion,
    ClusterWithFailure,
    PreventDeleteError,
    NotAllNodesUsed,
    SingleNode,
    Sharded,
} = composeStories(stories);

const selectors = {
    pageReady: /add node/i,
    reorderNodes: /reorder nodes/i,
    saveReorder: /save reorder/i,
    deleting: /deleting/i,
    settings: /settings/i,
    deleteFromGroup: /delete from group/i,
};

function getNodeItem(container: HTMLElement, nodeTag: string) {
    return within(container)
        .getByText(nodeTag, { selector: ".dbgroup-node strong" })
        .closest<HTMLElement>(".dbgroup-item");
}

async function chooseDeleteFromGroupOption(
    fireClick: (element: HTMLElement) => Promise<void>,
    nodeItem: HTMLElement,
    option: "Soft Delete" | "Hard Delete"
) {
    await fireClick(within(nodeItem).getByRole("button", { name: selectors.deleteFromGroup }));
    await fireClick(within(nodeItem).getByText(option));
}

describe("ManageDatabaseGroup", function () {
    it("can render cluster view", async () => {
        const { screen } = rtlRender(<Cluster />);

        await screen.findByText(selectors.pageReady);

        // settings should be available (Allow dynamic database distribution)
        await screen.findByText(selectors.settings);
    });

    it("can render sharded view", async () => {
        const { screen } = rtlRender(<Sharded />);

        expect(await screen.findAllByText(selectors.pageReady)).toHaveLength(4);

        // settings should NOT be available (Allow dynamic database distribution)
        expect(await screen.queryByText(selectors.settings)).not.toBeInTheDocument();
    });

    it("can render database with prevent delete (ignore)", async () => {
        const { screen } = rtlRender(<PreventDeleteIgnore />);

        await screen.findByText(selectors.pageReady);
    });

    it("can render database with prevent delete (error)", async () => {
        const { screen } = rtlRender(<PreventDeleteError />);

        await screen.findByText(selectors.pageReady);
    });

    it("can render delete in progress stage", async () => {
        const { screen } = rtlRender(<ClusterWithDeletion />);

        await screen.findByText(selectors.pageReady);
        expect(await screen.findAllByText(selectors.deleting)).toHaveLength(2);
    });

    it("can render cluster with failures", async () => {
        const { screen } = rtlRender(<ClusterWithFailure />);

        await screen.findByText(selectors.pageReady);

        await screen.findByText(/Rehab/);
    });

    it("can render single node", async () => {
        const { screen } = rtlRender(<SingleNode />);

        await screen.findByText(selectors.pageReady);
    });

    it("can render add more nodes", async () => {
        const { screen } = rtlRender(<NotAllNodesUsed />);

        await screen.findByText(selectors.pageReady);

        const addNodeButton = await screen.findByText(/add node/i);
        expect(addNodeButton).toBeEnabled();
    });

    it("can enter/exit reorder mode", async () => {
        const { screen, fireClick } = rtlRender(<Cluster />);

        await screen.findByText(selectors.pageReady);

        await fireClick(await screen.findByText(selectors.reorderNodes));
        await fireClick(await screen.findByText(selectors.saveReorder));
    });

    it("can set dirty flag", async () => {
        const { screen, fireClick } = rtlRender(<Cluster />);
        const { useDirtyFlag } = mockHooks;

        expect(useDirtyFlag.isDirty).toBeFalse();

        await screen.findByText(selectors.pageReady);

        await fireClick(await screen.findByText(selectors.reorderNodes));
        expect(useDirtyFlag.isDirty).toBeTrue();

        await fireClick(await screen.findByText(selectors.saveReorder));
        expect(useDirtyFlag.isDirty).toBeFalse();
    });

    describe("delete from group", () => {
        const deleteDatabaseFromNode = () => jest.mocked(mockServices.databasesService.mock.deleteDatabaseFromNode);

        beforeEach(() => {
            deleteDatabaseFromNode().mockClear();
        });

        it("soft deletes a node only after DELETE is typed", async () => {
            const { screen, container, fireClick, fillInput } = rtlRender(<Cluster />);
            await screen.findByText(selectors.pageReady);

            await chooseDeleteFromGroupOption(fireClick, getNodeItem(container, "B"), "Soft Delete");

            const dialog = await screen.findByRole("dialog");
            const softDeleteButton = within(dialog).getByRole("button", { name: "Soft delete" });
            expect(softDeleteButton).toBeDisabled();

            await fillInput(within(dialog).getByPlaceholderText("DELETE"), "DELETE");
            await fireClick(softDeleteButton);

            await waitFor(() => expect(deleteDatabaseFromNode()).toHaveBeenCalledWith("db1", ["B"], false));
            await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
        });

        it("lists what a soft delete does, including the tombstone risk", async () => {
            const { screen, container, fireClick } = rtlRender(<Cluster />);
            await screen.findByText(selectors.pageReady);

            await chooseDeleteFromGroupOption(fireClick, getNodeItem(container, "B"), "Soft Delete");

            const dialog = await screen.findByRole("dialog");
            expect(within(dialog).getByRole("list")).toHaveTextContent(/tombstones/i);
        });

        it("switches from soft delete to the hard delete confirmation", async () => {
            const { screen, container, fireClick } = rtlRender(<Cluster />);
            await screen.findByText(selectors.pageReady);

            await chooseDeleteFromGroupOption(fireClick, getNodeItem(container, "B"), "Soft Delete");

            const softDeleteDialog = await screen.findByRole("dialog");
            await fireClick(within(softDeleteDialog).getByRole("button", { name: /use hard delete instead/i }));
            await fireClick(await screen.findByRole("button", { name: "Delete" }));

            await waitFor(() => expect(deleteDatabaseFromNode()).toHaveBeenCalledWith("db1", ["B"], true));
            expect(deleteDatabaseFromNode()).not.toHaveBeenCalledWith("db1", ["B"], false);
        });

        it("keeps the node when soft delete is cancelled", async () => {
            const { screen, container, fireClick, fillInput } = rtlRender(<Cluster />);
            await screen.findByText(selectors.pageReady);

            await chooseDeleteFromGroupOption(fireClick, getNodeItem(container, "B"), "Soft Delete");

            const dialog = await screen.findByRole("dialog");
            await fillInput(within(dialog).getByPlaceholderText("DELETE"), "DELETE");
            await fireClick(within(dialog).getByRole("button", { name: "Cancel" }));

            await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
            expect(deleteDatabaseFromNode()).not.toHaveBeenCalled();
        });

        it("soft deletes a shard node only after DELETE is typed", async () => {
            const { screen, fireClick, fillInput } = rtlRender(<Sharded />);
            await screen.findAllByText(selectors.pageReady);

            const shardPanel = screen.getByText("Shard #1").closest<HTMLElement>(".rich-panel-item");
            await chooseDeleteFromGroupOption(fireClick, getNodeItem(shardPanel, "C"), "Soft Delete");

            const dialog = await screen.findByRole("dialog");
            const softDeleteButton = within(dialog).getByRole("button", { name: "Soft delete" });
            expect(softDeleteButton).toBeDisabled();

            await fillInput(within(dialog).getByPlaceholderText("DELETE"), "DELETE");
            await fireClick(softDeleteButton);

            await waitFor(() => expect(deleteDatabaseFromNode()).toHaveBeenCalledWith("sharded$1", ["C"], false));
            await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
        });

        it("links the backup and export pages of the whole database", async () => {
            const { screen, fireClick } = rtlRender(<Sharded />);
            await screen.findAllByText(selectors.pageReady);

            const shardPanel = screen.getByText("Shard #1").closest<HTMLElement>(".rich-panel-item");
            await chooseDeleteFromGroupOption(fireClick, getNodeItem(shardPanel, "C"), "Soft Delete");

            const dialog = await screen.findByRole("dialog");
            expect(within(dialog).getByRole("link", { name: "Back up" })).toHaveAttribute(
                "href",
                "#databases/tasks/backups?&database=sharded"
            );
            expect(within(dialog).getByRole("link", { name: "export" })).toHaveAttribute(
                "href",
                "#databases/tasks/exportDatabase?&database=sharded"
            );
        });

        it("hard deletes the only node only after DELETE is typed", async () => {
            const { screen, container, fireClick, fillInput } = rtlRender(<SingleNode />);
            await screen.findByText(selectors.pageReady);

            await chooseDeleteFromGroupOption(fireClick, getNodeItem(container, "A"), "Hard Delete");

            const dialog = await screen.findByRole("dialog");
            const hardDeleteButton = within(dialog).getByRole("button", { name: "Hard delete" });
            expect(hardDeleteButton).toBeDisabled();

            await fillInput(within(dialog).getByPlaceholderText("DELETE"), "DELETE");
            await fireClick(hardDeleteButton);

            await waitFor(() => expect(deleteDatabaseFromNode()).toHaveBeenCalledWith("db1", ["A"], true));
            await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
        });

        it("soft deletes the only node without suggesting Hard Delete", async () => {
            const { screen, container, fireClick, fillInput } = rtlRender(<SingleNode />);
            await screen.findByText(selectors.pageReady);

            await chooseDeleteFromGroupOption(fireClick, getNodeItem(container, "A"), "Soft Delete");

            const dialog = await screen.findByRole("dialog");
            expect(within(dialog).queryByRole("button", { name: /use hard delete instead/i })).not.toBeInTheDocument();

            await fillInput(within(dialog).getByPlaceholderText("DELETE"), "DELETE");
            await fireClick(within(dialog).getByRole("button", { name: "Soft delete" }));

            await waitFor(() => expect(deleteDatabaseFromNode()).toHaveBeenCalledWith("db1", ["A"], false));
            await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
        });

        it("hard deletes a node after a single confirmation", async () => {
            const { screen, container, fireClick } = rtlRender(<Cluster />);
            await screen.findByText(selectors.pageReady);

            await chooseDeleteFromGroupOption(fireClick, getNodeItem(container, "B"), "Hard Delete");

            const dialog = await screen.findByRole("dialog");
            await fireClick(within(dialog).getByRole("button", { name: "Delete" }));

            await waitFor(() => expect(deleteDatabaseFromNode()).toHaveBeenCalledWith("db1", ["B"], true));
        });
    });
});
