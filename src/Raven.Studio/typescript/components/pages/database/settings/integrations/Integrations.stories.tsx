import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import {
    withStorybookContexts,
    withBootstrap5,
    databaseAccessArgType,
    withForceRerender,
} from "test/storybookTestUtils";
import Integrations from "./Integrations";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { DatabaseSharedInfo } from "components/models/databases";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Database/Settings/Integrations",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface IntegrationsStoryArgs {
    databaseAccess: databaseAccessLevel;
    isSharded: boolean;
    hasPowerBi: boolean;
    hasPostgreSqlIntegration: boolean;
    credentialsDto: Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlUsernames;
    isPostgreSqlSupport: boolean;
}

export const IntegrationsStory: StoryObj<IntegrationsStoryArgs> = {
    name: "Integrations",
    render: (args) => {
        const { databases, accessManager, license } = mockStore;
        const { databasesService } = mockServices;

        let db: DatabaseSharedInfo = null;

        if (args.isSharded) {
            db = databases.withActiveDatabase_Sharded();
        } else {
            db = databases.withActiveDatabase_NonSharded_SingleNode();
        }

        accessManager.with_securityClearance("ValidUser");
        accessManager.with_databaseAccess({
            [db.name]: args.databaseAccess,
        });

        license.with_License({
            HasPowerBI: args.hasPowerBi,
            HasPostgreSqlIntegration: args.hasPostgreSqlIntegration,
        });

        databasesService.withIntegrationsPostgreSqlSupport(args.isPostgreSqlSupport);
        databasesService.withIntegrationsPostgreSqlCredentials(args.credentialsDto);
        databasesService.withGenerateSecret();

        return <Integrations />;
    },
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
    args: {
        databaseAccess: "DatabaseAdmin",
        isSharded: false,
        hasPowerBi: true,
        hasPostgreSqlIntegration: true,
        isPostgreSqlSupport: true,
        credentialsDto: DatabasesStubs.integrationsPostgreSqlCredentials(),
    },
};
