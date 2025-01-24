import { Meta, StoryObj } from "@storybook/react";
import React from "react";
import { mockStore } from "test/mocks/store/MockStore";
import { withStorybookContexts, withBootstrap5, databaseAccessArgType } from "test/storybookTestUtils";
import RevertRevisions from "./RevertRevisions";

export default {
    title: "Pages/Settings/Document Revisions/RevertRevisions",
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
} satisfies Meta;

interface DefaultRevertRevisionsProps {
    databaseAccess: databaseAccessLevel;
}

export const DefaultRevertRevisions: StoryObj<DefaultRevertRevisionsProps> = {
    name: "Revert Revisions",
    render: ({ databaseAccess }: DefaultRevertRevisionsProps) => {
        const { collectionsTracker, accessManager, databases } = mockStore;

        const db = databases.withActiveDatabase_NonSharded_SingleNode();

        accessManager.with_securityClearance("ValidUser");
        accessManager.with_databaseAccess({
            [db.name]: databaseAccess,
        });

        collectionsTracker.with_Collections();

        return <RevertRevisions />;
    },
    args: {
        databaseAccess: "DatabaseAdmin",
    },
};
