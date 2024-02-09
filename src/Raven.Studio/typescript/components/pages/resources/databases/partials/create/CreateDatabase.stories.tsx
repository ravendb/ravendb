import { Meta, StoryObj } from "@storybook/react";
import CreateDatabase from "./CreateDatabase";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { ResourcesStubs } from "test/stubs/ResourcesStubs";

export default {
    title: "Pages/Databases/Create Database/Create Database",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const DefaultCreateDatabase: StoryObj = {
    name: "Create Database",
    render: () => {
        const { license, accessManager, cluster } = mockStore;
        const { resourcesService } = mockServices;

        resourcesService.withValidateNameCommand(ResourcesStubs.invalidValidateName());
        resourcesService.withDatabaseLocation();
        resourcesService.withValidateNameCommand();
        resourcesService.withFolderPathOptions_ServerLocal();
        resourcesService.withRestorePoints_Local();

        license.with_License({
            HasEncryption: true,
        });

        accessManager.with_isServerSecure(true);
        cluster.with_Cluster();

        return <CreateDatabase closeModal={() => null} />;
    },
};
