﻿import React from "react";
import { Meta } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DocumentRevisions from "./DocumentRevisions";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings/DocumentRevisions",
    component: DocumentRevisions,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DocumentRevisions>;

const db = DatabasesStubs.nonShardedClusterDatabase();

function commonInit() {
    const { collectionsTracker, accessManager } = mockStore;
    const { databasesService } = mockServices;

    accessManager.with_securityClearance("ValidUser");

    collectionsTracker.with_Collections();

    databasesService.withRevisionsConfiguration();
    databasesService.withRevisionsForConflictsConfiguration();
}

export function DatabaseAdmin() {
    commonInit();
    const { accessManager } = mockStore;

    accessManager.with_databaseAccess({
        [db.name]: "DatabaseAdmin",
    });

    return <DocumentRevisions db={db} />;
}

export function BelowDatabaseAdmin() {
    commonInit();
    const { accessManager } = mockStore;

    accessManager.with_databaseAccess({
        [db.name]: "DatabaseRead",
    });

    return <DocumentRevisions db={db} />;
}

export function LicenseRestricted() {
    commonInit();
    const { accessManager } = mockStore;
    const { license } = mockStore;

    accessManager.with_databaseAccess({
        [db.name]: "DatabaseAdmin",
    });
    license.with_Community();

    return <DocumentRevisions db={db} />;
}
