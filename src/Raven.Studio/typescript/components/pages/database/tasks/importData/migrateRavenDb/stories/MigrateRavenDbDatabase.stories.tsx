import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { expect, userEvent, waitFor } from "storybook/test";
import {
    databaseAccessArgType,
    licenseArgType,
    securityClearanceArgType,
    withBootstrap5,
    withForceRerender,
    withStorybookContexts,
} from "test/storybookTestUtils";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import MigrateRavenDbDatabase from "components/pages/database/tasks/importData/migrateRavenDb/MigrateRavenDbDatabase";

type BuildInfoWithResourceNames = Raven.Server.Smuggler.Migration.BuildInfoWithResourceNames;
type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
type LicenseType = Raven.Server.Commercial.LicenseType;
type SourceVersion = "V7" | "V54" | "V41" | "V35";

interface MigrateRavenDbStoryArgs {
    databaseAccess: databaseAccessLevel;
    securityClearance: SecurityClearance;
    licenseType: LicenseType;
    hasAllLicenseFeatures: boolean;
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

const SERVER_URL = "http://raven-old-1:8080";

const SERVER_ERROR_MESSAGE =
    "Could not connect to the source server: System.Net.Http.HttpRequestException: No connection could be made " +
    "because the target machine actively refused it. (raven-old-1:8080) ---> System.Net.Sockets.SocketException " +
    "(10061): No connection could be made because the target machine actively refused it.";

function init({
    databaseAccess,
    securityClearance,
    licenseType,
    hasAllLicenseFeatures,
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
    if (hasAllLicenseFeatures) {
        license.with_License({ Type: licenseType });
    } else {
        license.with_LicenseLimited({ Type: licenseType });
    }

    tasksService.withMigrateRavenDb({
        buildInfo: {
            ...SOURCE_BUILD_INFOS[sourceVersion],
            ...(hasLongValues && { DatabaseNames: LONG_DATABASE_NAMES }),
        },
        buildInfoError: hasServerError ? SERVER_ERROR_MESSAGE : undefined,
        serverUrls: hasLongValues ? [LONG_SERVER_URL] : undefined,
    });
}

export default {
    title: "Pages/Tasks/Import Data/Migrate RavenDB",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    argTypes: {
        databaseAccess: databaseAccessArgType,
        securityClearance: securityClearanceArgType,
        licenseType: licenseArgType,
        sourceVersion: { control: "select", options: ["V7", "V54", "V41", "V35"] satisfies SourceVersion[] },
    },
    args: {
        databaseAccess: "DatabaseAdmin",
        securityClearance: "Operator",
        licenseType: "Enterprise",
        hasAllLicenseFeatures: true,
        sourceVersion: "V7",
        hasServerError: false,
        isSharded: false,
        hasLongValues: false,
    },
    render: (args) => {
        init(args);
        return <MigrateRavenDbDatabase />;
    },
    play: async ({ canvas, args }) => {
        const serverUrlInput = canvas.queryByRole("combobox", { name: "Server URL" });
        if (!serverUrlInput) {
            return;
        }
        await userEvent.type(serverUrlInput, args.hasLongValues ? LONG_SERVER_URL : SERVER_URL);
        await waitFor(
            () => expect(canvas.queryByRole("status", { name: "Detecting server version" })).not.toBeInTheDocument(),
            { timeout: 3000 }
        );
        if (args.hasLongValues) {
            await userEvent.type(canvas.getByRole("combobox", { name: "Database name" }), LONG_DATABASE_NAMES[0]);
        }
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

export const LicenseRestricted: Story = {
    args: { licenseType: "Professional", hasAllLicenseFeatures: false },
};
