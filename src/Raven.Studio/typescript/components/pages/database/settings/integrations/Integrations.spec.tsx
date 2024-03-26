import React from "react";
import { rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import * as Stories from "./Integrations.stories";
import { composeStories } from "@storybook/react";
type PostgreSqlUsernames = Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlUsernames;

const { IntegrationsStory } = composeStories(Stories);

const selectors = {
    richPanelActionsClass: "rich-panel-actions",
    addButtonText: /Add new/,
    postgreSqlMustBeEnabledText: /PostgreSQL support must be explicitly enabled/,
    licenseUpgradeRequiredText:
        /To use this feature, your license must include either of the following features: PostgreSQL integration or Power BI./,
};

describe("Integrations", () => {
    it("can render empty list", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IntegrationsStory
                credentialsDto={{
                    Users: [],
                }}
            />
        );

        expect(screen.getByText(/No credentials/)).toBeInTheDocument();
    });

    it("can render full list", async () => {
        const expectedUsername1 = "expected-user-1";
        const expectedUsername2 = "expected-user-2";

        const credentialsDto: PostgreSqlUsernames = {
            Users: [
                {
                    Username: expectedUsername1,
                },
                {
                    Username: expectedUsername2,
                },
            ],
        };

        const { screen } = await rtlRender_WithWaitForLoad(<IntegrationsStory credentialsDto={credentialsDto} />);

        expect(screen.getByText(expectedUsername1)).toBeInTheDocument();
        expect(screen.getByText(expectedUsername2)).toBeInTheDocument();
    });

    it("can hide buttons when has not database admin access", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<IntegrationsStory databaseAccess="DatabaseRead" />);

        expect(screen.queryByRole("button", { name: selectors.addButtonText })).toBeDisabled();
        expect(screen.queryByClassName(selectors.richPanelActionsClass)).not.toBeInTheDocument();
    });

    it("can show buttons when has database admin access", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<IntegrationsStory databaseAccess="DatabaseAdmin" />);

        expect(screen.queryByRole("button", { name: selectors.addButtonText })).toBeEnabled();
        expect(screen.queryAllByClassName(selectors.richPanelActionsClass)[0]).toBeInTheDocument();
    });

    it("can show alert when PostgreSQL support is not enabled", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<IntegrationsStory isPostgreSqlSupport={false} />);

        expect(screen.getByText(selectors.postgreSqlMustBeEnabledText)).toBeInTheDocument();
    });

    it("can show alert when license upgrade is required", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IntegrationsStory hasPowerBi={false} hasPostgreSqlIntegration={false} />
        );

        expect(screen.getByText(selectors.licenseUpgradeRequiredText)).toBeInTheDocument();
    });

    it("can render only license alert, when upgrade is required and PostgreSQL support is disabled", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IntegrationsStory hasPowerBi={false} hasPostgreSqlIntegration={false} isPostgreSqlSupport={false} />
        );

        expect(screen.getByText(selectors.licenseUpgradeRequiredText)).toBeInTheDocument();
        expect(screen.queryByText(selectors.postgreSqlMustBeEnabledText)).not.toBeInTheDocument();
    });

    it("can render feature not available for sharded database", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<IntegrationsStory isSharded={true} />);

        expect(screen.getByText(/Integrations are not available/)).toBeInTheDocument();
    });
});
