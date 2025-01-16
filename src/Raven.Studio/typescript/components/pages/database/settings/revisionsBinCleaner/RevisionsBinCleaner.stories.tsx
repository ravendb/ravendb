import { databaseAccessArgType, withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import RevisionsBinCleaner from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleaner";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import React from "react";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Database/Settings/Revisions Bin Cleaner",
    component: RevisionsBinCleaner,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof RevisionsBinCleaner>;

interface RevisionsBinCleanerStoryArgs {
    databaseAccess: databaseAccessLevel;
    revisionsBinCleanerDto: Raven.Client.Documents.Operations.Revisions.RevisionsBinConfiguration;
}

function commonInit(dto: Raven.Client.Documents.Operations.Revisions.RevisionsBinConfiguration) {
    const { databasesService } = mockServices;

    databasesService.withRevisionsBinCleanerConfiguration(dto);
}

export const DefaultRevisionsBinCleaner: StoryObj<RevisionsBinCleanerStoryArgs> = {
    name: "Revisions Bin Cleaner",
    render: (args) => {
        const { accessManager, databases } = mockStore;

        commonInit(args.revisionsBinCleanerDto);
        const db = databases.withActiveDatabase_NonSharded_SingleNode();

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
