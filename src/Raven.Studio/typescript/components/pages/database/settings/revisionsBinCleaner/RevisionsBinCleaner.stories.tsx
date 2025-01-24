import { databaseAccessArgType, withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import RevisionsBinCleaner from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleaner";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import React from "react";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Settings/Revisions Bin Cleaner",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface RevisionsBinCleanerStoryArgs {
    databaseAccess: databaseAccessLevel;
    revisionsBinCleanerDto: Raven.Client.Documents.Operations.Revisions.RevisionsBinConfiguration;
}

export const DefaultRevisionsBinCleaner: StoryObj<RevisionsBinCleanerStoryArgs> = {
    name: "Revisions Bin Cleaner",
    render: (args) => {
        const { accessManager, databases } = mockStore;
        const { databasesService } = mockServices;
        const db = databases.withActiveDatabase_NonSharded_SingleNode();

        databasesService.withRevisionsBinCleanerConfiguration(args.revisionsBinCleanerDto);

        accessManager.with_databaseAccess({
            [db.name]: args.databaseAccess,
        });

        return <RevisionsBinCleaner />;
    },
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
    args: {
        databaseAccess: "DatabaseAdmin",
        revisionsBinCleanerDto: DatabasesStubs.revisionsBinCleaner(),
    },
};
