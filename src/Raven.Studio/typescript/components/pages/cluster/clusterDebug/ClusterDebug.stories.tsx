import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ClusterDebug from "./ClusterDebug";

export default {
    title: "Pages/Cluster Debug",
    component: ClusterDebug,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof ClusterDebug>;

export const Default: StoryObj<typeof ClusterDebug> = {
    render: () => {
        return <ClusterDebug />;
    },
};
