import React from "react";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import StudioDatabaseConfiguration from "./StudioDatabaseConfiguration";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Database/Settings",
    component: StudioDatabaseConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof StudioDatabaseConfiguration>;

export const StudioConfiguration: ComponentStory<typeof StudioDatabaseConfiguration> = () => {
    return <StudioDatabaseConfiguration db={DatabasesStubs.nonShardedClusterDatabase()} />;
};
