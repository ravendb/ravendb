import DocumentExpiration from "components/pages/database/settings/documentExpiration/DocumentExpiration";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import RevisionsBinCleaner from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleaner";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import React from "react";

export default {
    title: "Pages/Database/Settings/Revisions Bin Cleaner",
    component: DocumentExpiration,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof RevisionsBinCleaner>;

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

export const DefaultRevisionsBinCleaner: StoryObj<typeof RevisionsBinCleaner> = {
    name: "Revisions Bin Cleaner",
    render: () => {
        commonInit(true);

        const { license } = mockStore;
        license.with_License();

        return <RevisionsBinCleaner />;
    },
};
