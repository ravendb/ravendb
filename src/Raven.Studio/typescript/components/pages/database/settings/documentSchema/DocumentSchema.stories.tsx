import { databaseAccessArgType, withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import DocumentSchema from "components/pages/database/settings/documentSchema/DocumentSchema";
import { mockStore } from "test/mocks/store/MockStore";
import DocumentSchemaPlayground from "components/pages/database/settings/documentSchema/partials/DocumentSchemaPlayground";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Settings/Document Schema",
    component: DocumentSchema,
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/dgThwcxBTCe0ABSdm6IxUX/Pages---Document-Schema",
        },
    },
} satisfies Meta;

interface DefaultDocumentSchemaArgs {
    databaseAccess: databaseAccessLevel;
}

export const DefaultDocumentSchema: StoryObj = {
    name: "Document Schema",
    render: (args: DefaultDocumentSchemaArgs) => {
        const { databases, accessManager } = mockStore;
        const { databasesService } = mockServices;
        const db = databases.withActiveDatabase_NonSharded_SingleNode();

        databasesService.withSchemaValidations();
        accessManager.with_databaseAccess({
            [db.name]: args.databaseAccess,
        });

        return <DocumentSchema />;
    },
};

export const DefaultDocumentSchemaPlayground: StoryObj = {
    name: "Document Schema Playground",
    render: () => {
        const { databases, accessManager } = mockStore;
        const db = databases.withActiveDatabase_NonSharded_SingleNode();

        accessManager.with_databaseAccess({
            [db.name]: "DatabaseAdmin",
        });

        return <DocumentSchemaPlayground />;
    },
};
