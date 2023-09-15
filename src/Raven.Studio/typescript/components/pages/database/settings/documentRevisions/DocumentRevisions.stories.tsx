import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, databaseAccessArgType, licenseArgType } from "test/storybookTestUtils";
import DocumentRevisions from "./DocumentRevisions";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings/DocumentRevisions",
    component: DocumentRevisions,
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        licenseType: licenseArgType,
        databaseAccess: databaseAccessArgType,
    },
} satisfies Meta<typeof DocumentRevisions>;

const db = DatabasesStubs.nonShardedClusterDatabase();

interface DefaultDocumentRevisionsProps {
    licenseType: Raven.Server.Commercial.LicenseType;
    isCloud: boolean;
    databaseAccess: databaseAccessLevel;
}

export const DefaultDocumentRevisions: StoryObj<DefaultDocumentRevisionsProps> = {
    name: "Document Revisions",
    render: ({ licenseType, isCloud, databaseAccess }: DefaultDocumentRevisionsProps) => {
        const { collectionsTracker, accessManager, license } = mockStore;
        const { databasesService } = mockServices;

        accessManager.with_securityClearance("ValidUser");

        collectionsTracker.with_Collections();

        databasesService.withRevisionsForConflictsConfiguration();
        databasesService.withRevisionsConfiguration();

        accessManager.with_databaseAccess({
            [db.name]: databaseAccess,
        });

        license.with_License({
            Type: licenseType,
            IsCloud: isCloud,
        });

        return <DocumentRevisions db={db} />;
    },
    args: {
        licenseType: "Community",
        isCloud: false,
        databaseAccess: "DatabaseAdmin",
    },
};
