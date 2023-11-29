import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, databaseAccessArgType } from "test/storybookTestUtils";
import UnusedDatabaseIds from "./UnusedDatabaseIds";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings",
    component: UnusedDatabaseIds,
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof UnusedDatabaseIds>;

const db = DatabasesStubs.nonShardedClusterDatabase();

interface DefaultUnusedDatabaseIdsProps {
    databaseAccess: databaseAccessLevel;
}
export const DefaultUnusedDatabaseIds: StoryObj<DefaultUnusedDatabaseIdsProps> = {
    name: "Unused Database IDs",
    render: ({ databaseAccess }: DefaultUnusedDatabaseIdsProps) => {
        const { accessManager } = mockStore;

        accessManager.with_securityClearance("ValidUser");

        accessManager.with_databaseAccess({
            [db.name]: databaseAccess,
        });

        return <UnusedDatabaseIds db={db} />;
    },
    args: {
        databaseAccess: "DatabaseAdmin",
    },
};
