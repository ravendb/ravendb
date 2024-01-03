import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, securityClearanceArgType } from "test/storybookTestUtils";
import DatabaseRecord from "./DatabaseRecord";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Database/Settings",
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        securityClearance: securityClearanceArgType,
    },
} satisfies Meta<typeof DatabaseRecord>;

interface DefaultDatabaseRecordProps {
    securityClearance: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
}

export const DefaultDatabaseRecord: StoryObj<DefaultDatabaseRecordProps> = {
    name: "Database Record",
    render: (props: DefaultDatabaseRecordProps) => {
        const { accessManager } = mockStore;
        const { databasesService } = mockServices;

        accessManager.with_securityClearance(props.securityClearance);
        databasesService.withDatabaseRecord();

        return <DatabaseRecord db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
    args: {
        securityClearance: "Operator",
    },
};
