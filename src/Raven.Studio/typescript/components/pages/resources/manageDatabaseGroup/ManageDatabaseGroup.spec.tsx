import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/react";

import * as stories from "./ManageDatabaseGroupPage.stories";
import { mockHooks } from "test/mocks/hooks/MockHooks";

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
};

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
});
