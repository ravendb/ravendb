import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, securityClearanceArgType } from "test/storybookTestUtils";
import DatabaseRecord from "./DatabaseRecord";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Settings/Database Record",
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        securityClearance: securityClearanceArgType,
    },
} satisfies Meta;

interface DefaultDatabaseRecordProps {
    securityClearance: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
}

export const DefaultDatabaseRecord: StoryObj<DefaultDatabaseRecordProps> = {
    name: "Database Record",
    render: (props: DefaultDatabaseRecordProps) => {
        const { accessManager, databases } = mockStore;
        const { databasesService } = mockServices;

        databases.withActiveDatabase_NonSharded_SingleNode();

        accessManager.with_securityClearance(props.securityClearance);
        databasesService.withDatabaseRecord();

        return <DatabaseRecord />;
    },
    args: {
        securityClearance: "Operator",
    },
};
