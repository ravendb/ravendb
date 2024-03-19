import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, databaseAccessArgType } from "test/storybookTestUtils";
import DatabaseCustomSorters from "./DatabaseCustomSorters";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";

export default {
    title: "Pages/Database/Settings/Custom Sorters",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

const databaseName = "databaseName";

interface DefaultDatabaseCustomSortersProps {
    isEmpty: boolean;
    databaseAccess: databaseAccessLevel;
    hasServerWideCustomSorters: boolean;
    maxNumberOfCustomSortersPerDatabase: number;
    maxNumberOfCustomSortersPerCluster: number;
}

export const DatabaseCustomSortersStory: StoryObj<DefaultDatabaseCustomSortersProps> = {
    name: "Custom Sorters",
    render: (props: DefaultDatabaseCustomSortersProps) => {
        const { accessManager, license } = mockStore;
        const { databasesService, manageServerService } = mockServices;

        accessManager.with_securityClearance("ValidUser");
        accessManager.with_databaseAccess({
            [db.name]: props.databaseAccess,
        });

        manageServerService.withServerWideCustomSorters(
            props.isEmpty ? [] : ManageServerStubs.serverWideCustomSorters()
        );

        databasesService.withEssentialStats();
        databasesService.withCustomSorters(props.isEmpty ? [] : DatabasesStubs.customSorters());
        databasesService.withQueryResult();

        license.with_LimitsUsage({
            NumberOfCustomSortersInCluster: ManageServerStubs.serverWideCustomSorters().length,
        });

        license.with_License({
            HasServerWideCustomSorters: props.hasServerWideCustomSorters,
            MaxNumberOfCustomSortersPerDatabase: props.maxNumberOfCustomSortersPerDatabase,
            MaxNumberOfCustomSortersPerCluster: props.maxNumberOfCustomSortersPerCluster,
        });

        return <DatabaseCustomSorters db={db} />;
    },
    args: {
        isEmpty: false,
        databaseAccess: "DatabaseAdmin",
        hasServerWideCustomSorters: true,
        maxNumberOfCustomSortersPerDatabase: 3,
        maxNumberOfCustomSortersPerCluster: 10,
    },
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
};
