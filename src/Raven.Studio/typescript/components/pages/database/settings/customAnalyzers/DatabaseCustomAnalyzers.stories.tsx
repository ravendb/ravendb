import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import {
    withStorybookContexts,
    withBootstrap5,
    databaseAccessArgType,
    withForceRerender,
} from "test/storybookTestUtils";
import DatabaseCustomAnalyzers from "./DatabaseCustomAnalyzers";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";

export default {
    title: "Pages/Settings/Custom Analyzers",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface DefaultDatabaseCustomAnalyzersProps {
    isEmpty: boolean;
    databaseAccess: databaseAccessLevel;
    hasServerWideCustomAnalyzers: boolean;
    maxNumberOfCustomAnalyzersPerDatabase: number;
    maxNumberOfCustomAnalyzersPerCluster: number;
}

export const DatabaseCustomAnalyzersStory: StoryObj<DefaultDatabaseCustomAnalyzersProps> = {
    name: "Custom Analyzers",
    render: (props: DefaultDatabaseCustomAnalyzersProps) => {
        const { accessManager, license, databases } = mockStore;
        const { databasesService, manageServerService } = mockServices;

        const db = databases.withActiveDatabase_NonSharded_SingleNode();

        accessManager.with_securityClearance("ValidUser");
        accessManager.with_databaseAccess({
            [db.name]: props.databaseAccess,
        });

        manageServerService.withServerWideCustomAnalyzers(
            props.isEmpty ? [] : ManageServerStubs.serverWideCustomAnalyzers()
        );

        databasesService.withEssentialStats();
        databasesService.withCustomAnalyzers(props.isEmpty ? [] : DatabasesStubs.customAnalyzers());
        databasesService.withQueryResult();

        license.with_LimitsUsage({
            NumberOfAnalyzersInCluster: ManageServerStubs.serverWideCustomAnalyzers().length,
        });

        license.with_License({
            HasServerWideAnalyzers: props.hasServerWideCustomAnalyzers,
            MaxNumberOfCustomAnalyzersPerDatabase: props.maxNumberOfCustomAnalyzersPerDatabase,
            MaxNumberOfCustomAnalyzersPerCluster: props.maxNumberOfCustomAnalyzersPerCluster,
        });

        return <DatabaseCustomAnalyzers />;
    },
    args: {
        isEmpty: false,
        databaseAccess: "DatabaseAdmin",
        hasServerWideCustomAnalyzers: true,
        maxNumberOfCustomAnalyzersPerDatabase: 3,
        maxNumberOfCustomAnalyzersPerCluster: 10,
    },
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
};
