import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ClusterDebug from "./ClusterDebug";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

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

function commonInit() {
    const { manageServerService } = mockServices;
    const { cluster } = mockStore;

    cluster.with_Cluster();

    manageServerService.withGetClusterLog();
    manageServerService.withGetClusterLogEntry();
}

const render = () => {
    commonInit();

    return <ClusterDebug />;
};

export const Default: StoryObj<typeof ClusterDebug> = {
    render,
};
