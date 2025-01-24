import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DataArchival from "./DataArchival";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Settings/Data Archival",
    component: DataArchival,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DataArchival>;

function commonInit() {
    const { databases } = mockStore;
    databases.withActiveDatabase_NonSharded_SingleNode();
}

export const LicenseAllowed: StoryObj<typeof DataArchival> = {
    render: () => {
        commonInit();

        const { databasesService } = mockServices;
        const { license } = mockStore;

        databasesService.withDataArchivalConfiguration();
        license.with_License();

        return <DataArchival />;
    },
};

export const LicenseRestricted: StoryObj<typeof DataArchival> = {
    render: () => {
        commonInit();

        const { databasesService } = mockServices;
        const { license } = mockStore;

        databasesService.withDataArchivalConfiguration();
        license.with_LicenseLimited({ HasDataArchival: false });

        return <DataArchival />;
    },
};
