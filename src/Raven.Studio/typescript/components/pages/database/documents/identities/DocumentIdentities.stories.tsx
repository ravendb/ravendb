import { databaseAccessArgType, withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import DocumentIdentities from "components/pages/database/documents/identities/DocumentIdentities";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Documents/Identities",
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/wSVoBtBHbk3qPgeYZ69co1/Pages---Identities?node-id=0-1&t=4iEd4mFqhniULAiu-1",
        },
    },
} satisfies Meta<typeof DocumentIdentities>;

interface DocumentIdentitiesStoryArgs {
    databaseAccess: databaseAccessLevel;
    identities: Record<string, number>;
}

export const DocumentIdentitiesStory: StoryObj<DocumentIdentitiesStoryArgs> = {
    name: "Identities",
    render: (args) => {
        const { accessManager, databases } = mockStore;

        const { name } = databases.withActiveDatabase_NonSharded_SingleNode();
        accessManager.with_databaseAccess({
            [name]: args.databaseAccess,
        });
        const { databasesService } = mockServices;

        databasesService.withIdentities(args.identities);

        return (
            <div style={{ height: "800px" }}>
                <DocumentIdentities />
            </div>
        );
    },
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
    args: {
        databaseAccess: "DatabaseRead",
        identities: DatabasesStubs.getIdentities(10),
    },
};
