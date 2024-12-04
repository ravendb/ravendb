import { databaseAccessArgType, withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import DocumentIdentities from "components/pages/database/documents/identities/DocumentIdentities";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Documents/Document Identities",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DocumentIdentities>;

interface DocumentIdentitiesStoryArgs {
    databaseAccess: databaseAccessLevel;
    identities: Record<string, number>;
}

export const DocumentIdentitiesStory: StoryObj<DocumentIdentitiesStoryArgs> = {
    name: "DocumentIdentities",
    render: (args) => {
        const { accessManager, license, databases } = mockStore;

        const db = databases.withActiveDatabase_NonSharded_SingleNode();
        accessManager.with_databaseAccess({
            [db.name]: args.databaseAccess,
        });
        license.with_License();
        const { databasesService } = mockServices;

        databasesService.withSampleIdentities(args.identities);

        return <DocumentIdentities />;
    },
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
    args: {
        databaseAccess: "DatabaseRead",
        identities: DatabasesStubs.getIdentities(10),
    },
};
