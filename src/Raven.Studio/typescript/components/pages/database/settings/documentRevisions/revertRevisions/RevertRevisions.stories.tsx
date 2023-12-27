import { Meta, StoryObj } from "@storybook/react";
import React from "react";
import { mockStore } from "test/mocks/store/MockStore";
import { withStorybookContexts, withBootstrap5, databaseAccessArgType } from "test/storybookTestUtils";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import RevertRevisions from "./RevertRevisions";

export default {
    title: "Pages/Database/Settings/DocumentRevisions/RevertRevisions",
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
} satisfies Meta<typeof RevertRevisions>;

const db = DatabasesStubs.nonShardedClusterDatabase();

interface DefaultRevertRevisionsProps {
    databaseAccess: databaseAccessLevel;
}

export const DefaultRevertRevisions: StoryObj<DefaultRevertRevisionsProps> = {
    name: "Revert Revisions",
    render: ({ databaseAccess }: DefaultRevertRevisionsProps) => {
        const { collectionsTracker, accessManager } = mockStore;

        accessManager.with_securityClearance("ValidUser");
        accessManager.with_databaseAccess({
            [db.name]: databaseAccess,
        });

        collectionsTracker.with_Collections();

        return <RevertRevisions db={db} />;
    },
    args: {
        databaseAccess: "DatabaseAdmin",
    },
};
