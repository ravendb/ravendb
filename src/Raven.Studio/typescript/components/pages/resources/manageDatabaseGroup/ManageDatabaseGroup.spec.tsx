import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/testing-react";

import * as stories from "./ManageDatabaseGroupPage.stories";

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
};

describe("ManageDatabaseGroup", function () {
    it("can render cluster view", async () => {
        const { screen } = rtlRender(<Cluster />);

        await screen.findByText(selectors.pageReady);
    });

    it("can render sharded view", async () => {
        const { screen } = rtlRender(<Sharded />);

        expect(await screen.findAllByText(selectors.pageReady)).toHaveLength(4);
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
    });

    it("can render cluster with failures", async () => {
        const { screen } = rtlRender(<ClusterWithFailure />);

        await screen.findByText(selectors.pageReady);
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
});
