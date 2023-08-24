import React from "react";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import StudioGlobalConfiguration from "./StudioGlobalConfiguration";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/ManageServer/Studio Configuration",
    component: StudioGlobalConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof StudioGlobalConfiguration>;

export const StudioConfiguration: ComponentStory<typeof StudioGlobalConfiguration> = () => {
    const { license } = mockStore;
    license.with_Enterprise();

    return <StudioGlobalConfiguration />;
};

export const LicenseRestricted: ComponentStory<typeof StudioGlobalConfiguration> = () => {
    const { license } = mockStore;
    license.with_Community();

    return <StudioGlobalConfiguration />;
};
