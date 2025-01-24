import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, databaseAccessArgType, licenseArgType } from "test/storybookTestUtils";
import DocumentCompression from "./DocumentCompression";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Settings/Document Compression",
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        licenseType: licenseArgType,
        databaseAccess: databaseAccessArgType,
    },
} satisfies Meta;

interface DefaultDocumentCompressionProps {
    licenseType: Raven.Server.Commercial.LicenseType;
    hasDocumentsCompression: boolean;
    databaseAccess: databaseAccessLevel;
}

export const DefaultDocumentCompression: StoryObj<DefaultDocumentCompressionProps> = {
    name: "Document Compression",
    render: ({ licenseType, hasDocumentsCompression, databaseAccess }: DefaultDocumentCompressionProps) => {
        const { collectionsTracker, accessManager, license, databases } = mockStore;
        const { databasesService } = mockServices;

        const db = databases.withActiveDatabase_NonSharded_SingleNode();

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

        return <DocumentCompression />;
    },
    args: {
        licenseType: "Enterprise",
        hasDocumentsCompression: true,
        databaseAccess: "DatabaseAdmin",
    },
};
