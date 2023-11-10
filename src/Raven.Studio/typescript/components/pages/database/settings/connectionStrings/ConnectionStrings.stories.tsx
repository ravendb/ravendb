import React from "react";
import { Meta } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ConnectionStrings from "./ConnectionStrings";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings/Connection Strings",
    component: ConnectionStrings,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof ConnectionStrings>;

const db = DatabasesStubs.nonShardedClusterDatabase();

function commonInit() {
    const { accessManager } = mockStore;
    accessManager.with_securityClearance("ValidUser");
}

export function NoLimits() {
    commonInit();

    const { accessManager } = mockStore;

    accessManager.with_databaseAccess({
        [db.name]: "DatabaseAdmin",
    });

    return <ConnectionStrings db={db} />;
}

export function BelowDatabaseAdmin() {
    commonInit();

    const { accessManager } = mockStore;

    accessManager.with_databaseAccess({
        [db.name]: "DatabaseRead",
    });

    return <ConnectionStrings db={db} />;
}
