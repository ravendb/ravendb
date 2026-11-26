import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import {
    databaseAccessArgType,
    securityClearanceArgType,
    withBootstrap5,
    withForceRerender,
    withStorybookContexts,
} from "test/storybookTestUtils";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import MigrateRavenDbDatabase from "../MigrateRavenDbDatabase";

type BuildInfoWithResourceNames = Raven.Server.Smuggler.Migration.BuildInfoWithResourceNames;
type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
type SourceVersion = "V7" | "V54" | "V41" | "V35";

interface MigrateRavenDbStoryArgs {
    databaseAccess: databaseAccessLevel;
    securityClearance: SecurityClearance;
    sourceVersion: SourceVersion;
    hasServerError: boolean;
    isSharded: boolean;
    hasLongValues: boolean;
}

const SOURCE_BUILD_INFOS: Record<SourceVersion, Partial<BuildInfoWithResourceNames>> = {
    V7: {},
    V54: { MajorVersion: "V5", BuildVersion: 54121, ProductVersion: "5.4", FullVersion: "5.4.121" },
    V41: { MajorVersion: "V4", BuildVersion: 41030, ProductVersion: "4.1", FullVersion: "4.1.10" },
    V35: {
        MajorVersion: "V35",
        BuildVersion: 35286,
        ProductVersion: "3.5",
        FullVersion: "",
        FileSystemNames: ["Files", "Backups"],
        IsLegacyOAuthToken: true,
    },
};

const LONG_SERVER_URL =
    "https://a-very-long-source-server-host-name.eu-central-1.ravendb.example.corporate-network.internal:8080";

const LONG_DATABASE_NAMES = [
    "Northwind-Production-Customers-And-Orders-Archive-Since-2015-Replicated-From-Europe-West",
    "Orders",
];

function init({
    databaseAccess,
    securityClearance,
    sourceVersion,
    hasServerError,
    isSharded,
    hasLongValues,
}: MigrateRavenDbStoryArgs) {
    const { accessManager, databases, license } = mockStore;
    const { tasksService } = mockServices;

    const db = isSharded
        ? databases.withActiveDatabase_Sharded()
        : databases.withActiveDatabase_NonSharded_SingleNode();

    accessManager.with_securityClearance(securityClearance);
    accessManager.with_databaseAccess({ [db.name]: databaseAccess });
    license.with_License();

    tasksService.withMigrateRavenDb({
        buildInfo: {
            ...SOURCE_BUILD_INFOS[sourceVersion],
            ...(hasLongValues && { DatabaseNames: LONG_DATABASE_NAMES }),
        },
        buildInfoError: hasServerError ? "Could not connect to the source server: connection refused" : undefined,
        serverUrls: hasLongValues ? [LONG_SERVER_URL] : undefined,
    });
}

export default {
    title: "Pages/Tasks/Import Data/Migrate RavenDB",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    argTypes: {
        databaseAccess: databaseAccessArgType,
        securityClearance: securityClearanceArgType,
        sourceVersion: { control: "select", options: ["V7", "V54", "V41", "V35"] satisfies SourceVersion[] },
    },
    args: {
        databaseAccess: "DatabaseAdmin",
        securityClearance: "Operator",
        sourceVersion: "V7",
        hasServerError: false,
        isSharded: false,
        hasLongValues: false,
    },
    render: (args) => {
        init(args);
        return <MigrateRavenDbDatabase />;
    },
} satisfies Meta<MigrateRavenDbStoryArgs>;

type Story = StoryObj<MigrateRavenDbStoryArgs>;

export const Default: Story = {
    name: "Migrate RavenDB",
};

export const Sharded: Story = {
    args: { isSharded: true },
};

export const NoOperator: Story = {
    args: { securityClearance: "ValidUser", databaseAccess: "DatabaseReadWrite" },
};

export const LegacyServer: Story = {
    args: { sourceVersion: "V35" },
};

export const ServerError: Story = {
    args: { hasServerError: true },
};

export const LongValues: Story = {
    args: { hasLongValues: true },
};
