import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ClusterDebug from "./ClusterDebug";

export default {
    title: "Pages/Manage Server/Advanced/Cluster Debug",
    component: ClusterDebug,
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/yw16WcguhZFtQsVICFP9M0/Pages---Cluster-Debugging?node-id=0-1&t=ppal0ndDWzpwtupp-1",
        },
    },
} satisfies Meta<typeof ClusterDebug>;

export const Default: StoryObj<typeof ClusterDebug> = {
    render: () => {
        return <ClusterDebug />;
    },
};
