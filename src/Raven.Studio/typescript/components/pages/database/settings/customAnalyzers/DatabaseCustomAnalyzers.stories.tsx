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

const db = DatabasesStubs.nonShardedDatabaseInfo();

function commonInit() {
    const { accessManager, databases } = mockStore;
    const { manageServerService, licenseService } = mockServices;

    databases.withActiveDatabase(db);

    accessManager.with_securityClearance("ValidUser");

    licenseService.withLimitsUsage();
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

    license.with_License();

    return <DatabaseCustomAnalyzers />;
}

export function BelowDatabaseAdmin() {
    commonInit();

    const { accessManager, license } = mockStore;
    const { databasesService } = mockServices;

    accessManager.with_databaseAccess({
        [db.name]: "DatabaseRead",
    });

    databasesService.withCustomAnalyzers();

    license.with_License();

    return <DatabaseCustomAnalyzers />;
}

export function LicenseLimits() {
    commonInit();

    const { accessManager, license } = mockStore;
    const { databasesService } = mockServices;

    accessManager.with_databaseAccess({
        [db.name]: "DatabaseAdmin",
    });

    databasesService.withCustomAnalyzers();

    license.with_LicenseLimited();
    license.with_LimitsUsage();

    return <DatabaseCustomAnalyzers />;
}
