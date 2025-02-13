import { Meta, StoryObj } from "@storybook/react";
import CreateDatabase from "./CreateDatabase";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Databases/Create Database/Create Database",
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/qRXxVe9VARbMIfMjin9fr8/Pages---Databases?node-id=14-3203",
        },
    },
} satisfies Meta;

interface DefaultCreateDatabaseProps {
    isSecureServer: boolean;
    hasEncryption: boolean;
    hasDynamicNodesDistribution: boolean;
    maxReplicationFactorForSharding: number;
}

export const DefaultCreateDatabase: StoryObj<DefaultCreateDatabaseProps> = {
    name: "Create Database",
    render: (props: DefaultCreateDatabaseProps) => {
        const { license, accessManager, cluster } = mockStore;
        const { resourcesService, databasesService } = mockServices;

        resourcesService.withValidateNameCommand();
        resourcesService.withDatabaseLocation();
        resourcesService.withFolderPathOptions_ServerLocal();
        resourcesService.withCloudBackupCredentialsFromLink();
        resourcesService.withRestorePoints();

        databasesService.withGenerateSecret();

        license.with_License({
            HasEncryption: props.hasEncryption,
            HasDynamicNodesDistribution: props.hasDynamicNodesDistribution,
            MaxReplicationFactorForSharding: props.maxReplicationFactorForSharding,
        });

        accessManager.with_isServerSecure(props.isSecureServer);
        cluster.with_Cluster();

        return <CreateDatabase closeModal={() => null} />;
    },
    args: {
        isSecureServer: true,
        hasEncryption: true,
        hasDynamicNodesDistribution: true,
        maxReplicationFactorForSharding: 1,
    },
};
