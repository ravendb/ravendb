import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, databaseAccessArgType, licenseArgType } from "test/storybookTestUtils";
import DocumentCompression from "./DocumentCompression";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings",
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        licenseType: licenseArgType,
        databaseAccess: databaseAccessArgType,
    },
} satisfies Meta<typeof DocumentCompression>;

const db = DatabasesStubs.nonShardedClusterDatabase();

interface DefaultDocumentCompressionProps {
    licenseType: Raven.Server.Commercial.LicenseType;
    hasDocumentsCompression: boolean;
    databaseAccess: databaseAccessLevel;
}

export const DefaultDocumentCompression: StoryObj<DefaultDocumentCompressionProps> = {
    name: "Document Compression",
    render: ({ licenseType, hasDocumentsCompression, databaseAccess }: DefaultDocumentCompressionProps) => {
        const { collectionsTracker, accessManager, license } = mockStore;
        const { databasesService } = mockServices;

        accessManager.with_securityClearance("ValidUser");
        collectionsTracker.with_Collections();
        databasesService.withDocumentsCompressionConfiguration();

        accessManager.with_databaseAccess({
            [db.name]: databaseAccess,
        });

        license.with_LicenseLimited({
            Type: licenseType,
            HasDocumentsCompression: hasDocumentsCompression,
        });

        return <DocumentCompression db={db} />;
    },
    args: {
        licenseType: "Enterprise",
        hasDocumentsCompression: true,
        databaseAccess: "DatabaseAdmin",
    },
};
