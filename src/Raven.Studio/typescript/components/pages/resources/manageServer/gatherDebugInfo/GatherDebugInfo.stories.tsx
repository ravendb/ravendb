import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import GatherDebugInfo from "./GatherDebugInfo";
import { mockStore } from "test/mocks/store/MockStore";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Manage Server/Gather Debug Info",
    component: GatherDebugInfo,
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/0AC6Rm0JBS5FBt3rsRKxOl/Pages---Debug-Package-Analyzer?node-id=232-7456",
        },
    },
} satisfies Meta<typeof GatherDebugInfo>;

export const DefaultGatherDebugInfo: StoryObj<typeof GatherDebugInfo> = {
    name: "Gather Debug Info",
    render: () => {
        const clusterDb = DatabasesStubs.nonShardedClusterDatabase().toDto();
        const shardedDb = DatabasesStubs.shardedDatabase().toDto();

        mockStore.databases.withDatabases([clusterDb, shardedDb]);

        return <GatherDebugInfo />;
    },
};
