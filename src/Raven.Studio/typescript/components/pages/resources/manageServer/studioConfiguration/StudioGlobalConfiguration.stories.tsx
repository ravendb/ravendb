import React from "react";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import StudioGlobalConfiguration from "./StudioGlobalConfiguration";

export default {
    title: "Pages/ManageServer/Studio Configuration",
    component: StudioGlobalConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof StudioGlobalConfiguration>;

export const StudioConfiguration: ComponentStory<typeof StudioGlobalConfiguration> = () => {
    return <StudioGlobalConfiguration />;
};

export const LicenseRestricted: ComponentStory<typeof StudioGlobalConfiguration> = () => {
    return <StudioGlobalConfiguration licenseType="community" />;
};
