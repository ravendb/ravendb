import React from "react";
import { Meta, StoryFn } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import StudioGlobalConfiguration from "./StudioGlobalConfiguration";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/ManageServer/Studio Configuration",
    component: StudioGlobalConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof StudioGlobalConfiguration>;

export const StudioConfiguration: StoryFn<typeof StudioGlobalConfiguration> = () => {
    const { license } = mockStore;
    license.with_License();

    return <StudioGlobalConfiguration />;
};

export const LicenseRestricted: StoryFn<typeof StudioGlobalConfiguration> = () => {
    const { license } = mockStore;
    license.with_LicenseLimited({ HasStudioConfiguration: false });

    return <StudioGlobalConfiguration />;
};
