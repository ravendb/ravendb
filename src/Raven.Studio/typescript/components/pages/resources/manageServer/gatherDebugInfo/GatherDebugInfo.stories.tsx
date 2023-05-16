import React from "react";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import GatherDebugInfo from "./GatherDebugInfo";

export default {
    title: "Pages/ManageServer",
    component: GatherDebugInfo,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof GatherDebugInfo>;

export const CreateDebugPackage: ComponentStory<typeof GatherDebugInfo> = () => {
    return <GatherDebugInfo />;
};
