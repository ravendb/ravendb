import React from "react";
import { ComponentMeta } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DatabaseRecord from "./DatabaseRecord";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings/Database Record",
    component: DatabaseRecord,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof DatabaseRecord>;

const db = DatabasesStubs.nonShardedClusterDatabase();

function commonInit() {
    const { accessManager } = mockStore;

    accessManager.with_securityClearance("ValidUser");
}

export function FullAccess() {
    commonInit();

    const { accessManager } = mockStore;
    accessManager.with_databaseAccess({
        [db.name]: "DatabaseAdmin",
    });

    return <DatabaseRecord db={db} />;
}

export function AccessForbidden() {
    commonInit();
    const { accessManager } = mockStore;
    accessManager.with_databaseAccess({
        [db.name]: "DatabaseRead",
    });
    return <DatabaseRecord db={db} />;
}
