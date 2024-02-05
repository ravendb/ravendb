import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, databaseAccessArgType } from "test/storybookTestUtils";
import ConflictResolution from "./ConflictResolution";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Database/Settings/Conflict Resolution",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof ConflictResolution>;

const db = DatabasesStubs.nonShardedClusterDatabase();

export const DefaultConflictResolution: StoryObj<{ databaseAccess: databaseAccessLevel }> = {
    name: "Conflict Resolution",
    render: (args) => {
        const { accessManager, collectionsTracker } = mockStore;
        const { databasesService } = mockServices;

        accessManager.with_securityClearance("ValidUser");
        accessManager.with_databaseAccess({
            [db.name]: args.databaseAccess,
        });

        collectionsTracker.with_Collections();
        databasesService.withConflictSolverConfiguration();

        return <ConflictResolution db={db} />;
    },
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
    args: {
        databaseAccess: "DatabaseAdmin",
    },
};
