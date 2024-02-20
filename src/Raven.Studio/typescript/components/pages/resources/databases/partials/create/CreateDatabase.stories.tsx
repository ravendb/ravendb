import { Meta, StoryObj } from "@storybook/react";
import CreateDatabase from "./CreateDatabase";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Databases/Create Database/Create Database",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const DefaultCreateDatabase: StoryObj = {
    name: "Create Database",
    render: () => {
        const { license, accessManager, cluster } = mockStore;
        const { resourcesService, databasesService } = mockServices;

        resourcesService.withValidateNameCommand();
        resourcesService.withDatabaseLocation();
        resourcesService.withFolderPathOptions_ServerLocal();
        resourcesService.withRestorePoints();

        databasesService.withGenerateSecret();

        license.with_License({
            HasEncryption: true,
        });

        accessManager.with_isServerSecure(true);
        cluster.with_Cluster();

        return <CreateDatabase closeModal={() => null} />;
    },
};
