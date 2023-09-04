import React from "react";
import { Meta } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DatabaseCustomAnalyzers from "./DatabaseCustomAnalyzers";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";

export default {
    title: "Pages/Database/Settings/Custom Analyzers",
    component: DatabaseCustomAnalyzers,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DatabaseCustomAnalyzers>;

const db = DatabasesStubs.nonShardedClusterDatabase();

function commonInit() {
    const { accessManager } = mockStore;
    const { manageServerService } = mockServices;

    accessManager.with_securityClearance("ValidUser");

    manageServerService.withServerWideCustomAnalyzers();
}

export function NoLimits() {
    commonInit();

    const { accessManager, license } = mockStore;
    const { databasesService } = mockServices;

    accessManager.with_databaseAccess({
        [db.name]: "DatabaseAdmin",
    });

    databasesService.withCustomAnalyzers([
        ...DatabasesStubs.customAnalyzers(),
        ManageServerStubs.serverWideCustomAnalyzers()[0],
    ]);

    license.with_Enterprise();

    return <DatabaseCustomAnalyzers db={db} />;
}

export function BelowDatabaseAdmin() {
    commonInit();

    const { accessManager, license } = mockStore;
    const { databasesService } = mockServices;

    accessManager.with_databaseAccess({
        [db.name]: "DatabaseRead",
    });

    databasesService.withCustomAnalyzers();

    license.with_Enterprise();

    return <DatabaseCustomAnalyzers db={db} />;
}

export function CommunityLimits() {
    commonInit();

    const { accessManager, license } = mockStore;
    const { databasesService } = mockServices;

    accessManager.with_databaseAccess({
        [db.name]: "DatabaseAdmin",
    });

    databasesService.withCustomAnalyzers();

    license.with_Community();

    return <DatabaseCustomAnalyzers db={db} />;
}
