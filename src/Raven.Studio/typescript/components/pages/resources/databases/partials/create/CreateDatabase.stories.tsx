import { Meta, StoryObj } from "@storybook/react-webpack5";
import CreateDatabase, { CreateDatabaseMode } from "./CreateDatabase";
import React, { useState } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import Button from "react-bootstrap/Button";

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
    singleNode: boolean;
}

export const DefaultCreateDatabase: StoryObj<DefaultCreateDatabaseProps> = {
    name: "Create Database",
    render: (props: DefaultCreateDatabaseProps) => {
        const [createDatabaseMode, setCreateDatabaseMode] = useState<CreateDatabaseMode>(null);
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

        if (props.singleNode) {
            cluster.with_Single();
        } else {
            cluster.with_Cluster();
        }

        return (
            <>
                <div className="vstack gap-4">
                    <Button
                        data-testid="open-create-database-modal-regular"
                        variant="primary"
                        onClick={() => setCreateDatabaseMode("regular")}
                    >
                        Open Create Database Modal Regular
                    </Button>
                    <Button
                        data-testid="open-create-database-modal-from-backup"
                        variant="primary"
                        onClick={() => setCreateDatabaseMode("fromBackup")}
                    >
                        Open Create Database Modal From Backup
                    </Button>
                </div>
                {createDatabaseMode && (
                    <CreateDatabase closeModal={() => setCreateDatabaseMode(null)} initialMode={createDatabaseMode} />
                )}
            </>
        );
    },
    args: {
        isSecureServer: true,
        hasEncryption: true,
        hasDynamicNodesDistribution: true,
        maxReplicationFactorForSharding: 1,
        singleNode: false,
    },
};
