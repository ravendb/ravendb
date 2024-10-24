import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DocumentExpiration from "./DocumentExpiration";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings/Document Expiration",
    component: DocumentExpiration,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DocumentExpiration>;

function commonInit(hasConfiguration: boolean) {
    const { databasesService } = mockServices;
    const { databases } = mockStore;

    if (hasConfiguration) {
        databasesService.withExpirationConfiguration();
    } else {
        databasesService.withoutExpirationConfiguration();
    }
    databases.withActiveDatabase_NonSharded_SingleNode();
}

export const DefaultDocumentExpiration: StoryObj<typeof DocumentExpiration> = {
    name: "Document Expiration",
    render: () => {
        commonInit(true);

        const { license } = mockStore;
        license.with_License();

        return <DocumentExpiration />;
    },
};

export const InitialDocumentExpiration: StoryObj<typeof DocumentExpiration> = {
    render: () => {
        commonInit(false);

        const { license } = mockStore;
        license.with_License();

        return <DocumentExpiration />;
    },
};

export const LicenseRestricted: StoryObj<typeof DocumentExpiration> = {
    render: () => {
        commonInit(true);

        const { license } = mockStore;
        license.with_LicenseLimited();

        return <DocumentExpiration />;
    },
};
