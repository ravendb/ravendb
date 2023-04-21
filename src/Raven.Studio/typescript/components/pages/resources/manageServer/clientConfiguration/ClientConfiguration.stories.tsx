import React from "react";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ClientConfiguration from "./ClientConfiguration";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/ClientConfiguration",
    component: ClientConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof ClientConfiguration>;

export const Primary: ComponentStory<typeof ClientConfiguration> = () => {
    const { manageServerService } = mockServices;

    manageServerService.withGetGlobalClientConfiguration();

    return <ClientConfiguration />;
};
