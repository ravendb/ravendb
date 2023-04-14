import React from "react";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ClientConfiguration from "./ClientConfiguration";

export default {
    title: "Pages/ClientConfiguration",
    component: ClientConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof ClientConfiguration>;

export const Primary: ComponentStory<typeof ClientConfiguration> = () => <ClientConfiguration />;
