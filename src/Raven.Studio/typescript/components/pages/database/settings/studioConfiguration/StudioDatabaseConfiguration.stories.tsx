import React from "react";
import { Meta, ComponentStory } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import StudioDatabaseConfiguration from "./StudioDatabaseConfiguration";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings/Studio Configuration",
    component: StudioDatabaseConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof StudioDatabaseConfiguration>;

function commonInit() {
    const { databases } = mockStore;
    databases.withActiveDatabase_NonSharded_SingleNode();
}

export const StudioConfiguration: ComponentStory<typeof StudioDatabaseConfiguration> = () => {
    commonInit();

    const { license } = mockStore;
    license.with_License();

    return <StudioDatabaseConfiguration />;
};

export const LicenseRestricted: ComponentStory<typeof StudioDatabaseConfiguration> = () => {
    commonInit();

    const { license } = mockStore;
    license.with_LicenseLimited({ HasStudioConfiguration: false });

    return <StudioDatabaseConfiguration />;
};
