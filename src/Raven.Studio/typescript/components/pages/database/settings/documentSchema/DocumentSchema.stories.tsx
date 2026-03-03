import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import DocumentSchema from "components/pages/database/settings/documentSchema/DocumentSchema";
import { mockStore } from "test/mocks/store/MockStore";
import DocumentSchemaPlayground from "components/pages/database/settings/documentSchema/partials/DocumentSchemaPlayground";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Settings/Document Schema",
    component: DocumentSchema,
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/dgThwcxBTCe0ABSdm6IxUX/Pages---Document-Schema",
        },
    },
    args: {
        hasLicense: true,
    },
} satisfies Meta;

interface DefaultDocumentSchemaArgs {
    hasLicense: boolean;
}

export const DefaultDocumentSchema: StoryObj<DefaultDocumentSchemaArgs> = {
    name: "Document Schema",
    render: (args) => {
        const { databases, accessManager, collectionsTracker, license } = mockStore;
        const { databasesService } = mockServices;

        const db = databases.withActiveDatabase_NonSharded_SingleNode();
        collectionsTracker.with_Collections();
        license.with_License({
            HasSchemaValidation: args.hasLicense,
        });
        databasesService.withSchemaValidations();
        accessManager.with_securityClearance("ClusterAdmin");

        return <DocumentSchema />;
    },
};

export const DefaultDocumentSchemaPlayground: StoryObj<DefaultDocumentSchemaArgs> = {
    name: "Document Schema Playground",
    render: (args) => {
        const { databases, accessManager, collectionsTracker, license } = mockStore;

        const db = databases.withActiveDatabase_NonSharded_SingleNode();
        collectionsTracker.with_Collections();
        license.with_License({
            HasSchemaValidation: args.hasLicense,
        });
        accessManager.with_databaseAccess({
            [db.name]: "DatabaseAdmin",
        });

        return <DocumentSchemaPlayground />;
    },
};
