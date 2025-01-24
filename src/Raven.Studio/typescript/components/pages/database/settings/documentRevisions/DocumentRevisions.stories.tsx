import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, databaseAccessArgType, licenseArgType } from "test/storybookTestUtils";
import DocumentRevisions from "./DocumentRevisions";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Settings/Document Revisions",
    component: DocumentRevisions,
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        licenseType: licenseArgType,
        databaseAccess: databaseAccessArgType,
    },
} satisfies Meta;

interface DefaultDocumentRevisionsProps {
    licenseType: Raven.Server.Commercial.LicenseType;
    isCloud: boolean;
    canSetupDefaultRevisionsConfiguration: boolean;
    maxNumberOfRevisionsToKeep: number;
    maxNumberOfRevisionAgeToKeepInDays: number;
    databaseAccess: databaseAccessLevel;
}

export const DefaultDocumentRevisions: StoryObj<DefaultDocumentRevisionsProps> = {
    name: "Document Revisions",
    render: ({
        licenseType,
        isCloud,
        canSetupDefaultRevisionsConfiguration,
        maxNumberOfRevisionsToKeep,
        maxNumberOfRevisionAgeToKeepInDays,
        databaseAccess,
    }: DefaultDocumentRevisionsProps) => {
        const { collectionsTracker, accessManager, license, databases } = mockStore;
        const { databasesService } = mockServices;

        const db = databases.withActiveDatabase_NonSharded_SingleNode();

        accessManager.with_securityClearance("ValidUser");

        collectionsTracker.with_Collections();

        databasesService.withRevisionsForConflictsConfiguration();
        databasesService.withRevisionsConfiguration();

        accessManager.with_databaseAccess({
            [db.name]: databaseAccess,
        });

        license.with_LicenseLimited({
            Type: licenseType,
            IsCloud: isCloud,
            CanSetupDefaultRevisionsConfiguration: canSetupDefaultRevisionsConfiguration,
            MaxNumberOfRevisionAgeToKeepInDays: maxNumberOfRevisionAgeToKeepInDays,
            MaxNumberOfRevisionsToKeep: maxNumberOfRevisionsToKeep,
        });

        return <DocumentRevisions />;
    },
    args: {
        licenseType: "Community",
        isCloud: false,
        canSetupDefaultRevisionsConfiguration: false,
        maxNumberOfRevisionsToKeep: 2,
        maxNumberOfRevisionAgeToKeepInDays: 45,
        databaseAccess: "DatabaseAdmin",
    },
};
