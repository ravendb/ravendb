import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/react";

import * as stories from "./DatabasesPage.stories";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const {
    Single,
    Cluster,
    Sharded,
    DifferentNodeStates,
    WithLoadErrorOnSingleNode,
    WithLoadErrorOnAllNodes,
    WithDifferentAccessLevel,
    WithOfflineNodes,
} = composeStories(stories);

const selectors = {
    clusterDatabaseName: DatabasesStubs.nonShardedClusterDatabase().name,
    disableButton: "Disable",
    enableButton: "Enable",
    pauseIndexing: "Pause indexing until restart",
    disableIndexing: "Disable indexing",
    compactDatabase: "Compact database",
    showFilteringOptions: "Show Filtering Options",
};

describe("DatabasesPage", function () {
    it("can render single view", async () => {
        const { screen } = rtlRender(<Single />);

        await screen.findByText(/Manage group/i);
        await screen.findByText("3 Indexing errors");

        expect(await screen.findAllByText(selectors.disableButton)).toHaveLength(2); // disable + disable indexing

        expect(await screen.findByText(selectors.pauseIndexing)).toBeInTheDocument();
        expect(await screen.findByText(selectors.disableIndexing)).toBeInTheDocument();
        expect(await screen.findByText(selectors.compactDatabase)).toBeInTheDocument();
    });

    it("can render cluster view", async () => {
        const { screen } = rtlRender(<Cluster />);

        await screen.findByText(/Manage group/i);

        await screen.findByText("9 Indexing errors");
    });

    it("can render load error on single node", async () => {
        const { screen, fireClick } = rtlRender(<WithLoadErrorOnSingleNode />);

        await screen.findByText(/Manage group/i);

        await screen.findByText("6 Indexing errors");
        await screen.findByText(/Database has load errors/i);
        const findAllDistributionDetailsTitle = await screen.findAllByTitle(/Expand distribution details/i);

        await fireClick(findAllDistributionDetailsTitle[0]);
    });

    it("can render load error on all nodes", async () => {
        const { screen } = rtlRender(<WithLoadErrorOnAllNodes />);

        // general info section is hidden
        expect(screen.queryByClassName("icon-documents")).not.toBeInTheDocument();
    });

    it("can render sharded view", async () => {
        const { screen } = rtlRender(<Sharded />);

        await screen.findByText(/Manage group/i);

        await screen.findByText("18 Indexing errors");
    });

    it("can render node statuses", async () => {
        const { screen } = rtlRender(<DifferentNodeStates />);

        expect(await screen.findAllByText(/Manage group/i)).toHaveLength(2);

        await screen.findByText("9 Indexing errors");
        await screen.findByText("18 Indexing errors");
    });

    it("can render different access modes", async () => {
        const { screen } = rtlRender(<WithDifferentAccessLevel />);

        expect(await screen.findAllByText("9 Indexing errors")).toHaveLength(3);

        expect(screen.queryByText(/Manage group/i)).not.toBeInTheDocument();
        expect(screen.queryByText(selectors.disableButton)).not.toBeInTheDocument();
        expect(screen.queryByText(selectors.enableButton)).not.toBeInTheDocument();
        expect(screen.queryByText(selectors.disableIndexing)).not.toBeInTheDocument();
        expect(screen.queryByText(selectors.compactDatabase)).not.toBeInTheDocument();

        // db admin can pause indexing
        expect(screen.queryByText(selectors.pauseIndexing)).toBeInTheDocument();
    });

    describe("filter by state", () => {
        const localNodeTag = "A";
        const remoteNodeTags = ["B", "C"];

        it("can filter local + offline", async () => {
            const { screen, fireClick } = rtlRender(<WithOfflineNodes offlineNodes={[localNodeTag]} />);

            await fireClick(await screen.findByRole("button", { name: selectors.showFilteringOptions }));

            await fireClick(screen.getByRole("checkbox", { name: /Local/ }));
            await fireClick(screen.getByRole("checkbox", { name: /Offline/ }));

            expect(screen.queryByRole("heading", { name: selectors.clusterDatabaseName })).toBeInTheDocument();
        });

        it("can filter local + online", async () => {
            const { screen, fireClick } = rtlRender(<WithOfflineNodes offlineNodes={[localNodeTag]} />);

            await fireClick(await screen.findByRole("button", { name: selectors.showFilteringOptions }));

            await fireClick(screen.getByRole("checkbox", { name: /Local/ }));
            await fireClick(screen.getByRole("checkbox", { name: /Online/ }));

            expect(screen.queryByRole("heading", { name: selectors.clusterDatabaseName })).not.toBeInTheDocument();
        });

        it("can filter remote + offline when all offline", async () => {
            const { screen, fireClick } = rtlRender(<WithOfflineNodes offlineNodes={remoteNodeTags} />);

            await fireClick(await screen.findByRole("button", { name: selectors.showFilteringOptions }));

            await fireClick(screen.getByRole("checkbox", { name: /Remote/ }));
            await fireClick(screen.getByRole("checkbox", { name: /Offline/ }));

            expect(screen.queryByRole("heading", { name: selectors.clusterDatabaseName })).toBeInTheDocument();
        });

        it("can filter remote + online when all offline", async () => {
            const { screen, fireClick } = rtlRender(<WithOfflineNodes offlineNodes={remoteNodeTags} />);

            await fireClick(await screen.findByRole("button", { name: selectors.showFilteringOptions }));

            await fireClick(screen.getByRole("checkbox", { name: /Remote/ }));
            await fireClick(screen.getByRole("checkbox", { name: /Online/ }));

            expect(screen.queryByRole("heading", { name: selectors.clusterDatabaseName })).not.toBeInTheDocument();
        });

        it("can filter remote + offline when some offline", async () => {
            const { screen, fireClick } = rtlRender(<WithOfflineNodes offlineNodes={[remoteNodeTags[0]]} />);

            await fireClick(await screen.findByRole("button", { name: selectors.showFilteringOptions }));

            await fireClick(screen.getByRole("checkbox", { name: /Remote/ }));
            await fireClick(screen.getByRole("checkbox", { name: /Offline/ }));

            expect(screen.queryByRole("heading", { name: selectors.clusterDatabaseName })).toBeInTheDocument();
        });

        it("can filter remote + online when some offline", async () => {
            const { screen, fireClick } = rtlRender(<WithOfflineNodes offlineNodes={[remoteNodeTags[0]]} />);

            await fireClick(await screen.findByRole("button", { name: selectors.showFilteringOptions }));

            await fireClick(screen.getByRole("checkbox", { name: /Remote/ }));
            await fireClick(screen.getByRole("checkbox", { name: /Online/ }));

            expect(screen.queryByRole("heading", { name: selectors.clusterDatabaseName })).toBeInTheDocument();
        });
    });
});
