import { rtlRender } from "test/rtlTestUtils";
import { composeStories } from "@storybook/react";
import * as stories from "./AboutPage.stories";
import React from "react";

const {
    AboutPage,
    ConnectionFailure,
    NoSupportOnPremise,
    NoSupportCloud,
    ProfessionalSupportOnPremise,
    ProfessionalSupportCloud,
    ProductionSupportCloud,
    ProductionSupportOnPremise,
    NoLicense,
    DeveloperLicense,
    ProfessionalLicense,
    EnterpriseLicense,
    EssentialLicense,
    CommunityLicense,
    UsingLatestVersion,
} = composeStories(stories);

describe("AboutPage", function () {
    describe("Licenses", function () {
        it("none - AGPL", async () => {
            const { screen } = rtlRender(<NoLicense />);

            expect(await screen.findByText(selectors.licenseServer.connected)).toBeInTheDocument();

            expect(screen.queryAllByText(selectors.agplLicense)).toHaveLength(2);
            expect(screen.queryByText(selectors.licenseId)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.renewLicense)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.registerLicense)).toBeInTheDocument();
            expect(screen.queryByText(selectors.forceUpdate)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.replaceLicense)).not.toBeInTheDocument();
        });

        it("enterprise", async () => {
            const { screen } = rtlRender(<EnterpriseLicense />);

            expect(await screen.findByText(selectors.licenseServer.connected)).toBeInTheDocument();

            expect(screen.queryByText(selectors.agplLicense)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.licenseId)).toBeInTheDocument();
            expect(screen.queryByText(selectors.renewLicense)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.registerLicense)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.forceUpdate)).toBeInTheDocument();
            expect(screen.queryByText(selectors.replaceLicense)).toBeInTheDocument();
        });

        it("professional", async () => {
            const { screen } = rtlRender(<ProfessionalLicense />);

            expect(await screen.findByText(selectors.licenseServer.connected)).toBeInTheDocument();

            expect(screen.queryByText(selectors.agplLicense)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.licenseId)).toBeInTheDocument();
            expect(screen.queryByText(selectors.renewLicense)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.registerLicense)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.forceUpdate)).toBeInTheDocument();
            expect(screen.queryByText(selectors.replaceLicense)).toBeInTheDocument();
        });

        it("community", async () => {
            const { screen } = rtlRender(<CommunityLicense />);

            expect(await screen.findByText(selectors.licenseServer.connected)).toBeInTheDocument();

            expect(screen.queryByText(selectors.agplLicense)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.licenseId)).toBeInTheDocument();
            expect(await screen.findByText(selectors.renewLicense)).toBeInTheDocument();
            expect(screen.queryByText(selectors.registerLicense)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.forceUpdate)).toBeInTheDocument();
            expect(screen.queryByText(selectors.replaceLicense)).toBeInTheDocument();
        });

        it("essential", async () => {
            const { screen } = rtlRender(<EssentialLicense />);

            expect(await screen.findByText(selectors.licenseServer.connected)).toBeInTheDocument();

            expect(screen.queryByText(selectors.agplLicense)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.licenseId)).toBeInTheDocument();
            expect(screen.queryByText(selectors.renewLicense)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.registerLicense)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.forceUpdate)).toBeInTheDocument();
            expect(screen.queryByText(selectors.replaceLicense)).toBeInTheDocument();
        });

        it("developer", async () => {
            const { screen } = rtlRender(<DeveloperLicense />);

            expect(await screen.findByText(selectors.licenseServer.connected)).toBeInTheDocument();

            expect(screen.queryByText(selectors.agplLicense)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.licenseId)).toBeInTheDocument();
            expect(await screen.findByText(selectors.renewLicense)).toBeInTheDocument();
            expect(screen.queryByText(selectors.registerLicense)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.forceUpdate)).toBeInTheDocument();
            expect(screen.queryByText(selectors.replaceLicense)).toBeInTheDocument();
        });
    });

    describe("latest version", function () {
        it("upgrade available", async function () {
            const { screen, fireClick } = rtlRender(<AboutPage />);

            expect(await screen.findByText(selectors.licenseServer.connected)).toBeInTheDocument();
            expect(screen.queryByText(selectors.versions.usingLatest)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.versions.whatsNew)).toBeInTheDocument();

            await fireClick(screen.queryByText(selectors.versions.whatsNew));

            expect(await screen.findByText(selectors.versions.close)).toBeInTheDocument();
            expect(await screen.findByText(selectors.versions.updateInstructions)).toBeInTheDocument();
        });

        it("using latest version", async function () {
            const { screen, fireClick } = rtlRender(<UsingLatestVersion />);

            expect(await screen.findByText(selectors.licenseServer.connected)).toBeInTheDocument();
            expect(screen.queryByText(selectors.versions.usingLatest)).toBeInTheDocument();
            expect(screen.queryByText(selectors.versions.whatsNew)).not.toBeInTheDocument();

            await fireClick(screen.queryByText(selectors.versions.changelog));

            expect(await screen.findByText(selectors.versions.close)).toBeInTheDocument();
            expect(screen.queryByText(selectors.versions.updateInstructions)).not.toBeInTheDocument();
        });
    });

    describe("support", function () {
        it("no support - on premise", async function () {
            const { screen, fireClick } = rtlRender(<NoSupportOnPremise />);

            expect(await screen.findByText(selectors.licenseServer.connected)).toBeInTheDocument();

            await fireClick(screen.queryByText(selectors.support.supportPlanTab));

            expect(screen.queryByText(selectors.support.productionSla)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.support.professionalSla)).not.toBeInTheDocument();

            expect(screen.queryByText(selectors.support.upgradeYourSupport)).toBeInTheDocument();
            expect(screen.queryByText(selectors.support.upgradeToProduction)).not.toBeInTheDocument();
        });

        it("no support - cloud", async function () {
            const { screen, fireClick } = rtlRender(<NoSupportCloud />);

            expect(await screen.findByText(selectors.licenseServer.connected)).toBeInTheDocument();

            await fireClick(screen.queryByText(selectors.support.supportPlanTab));

            expect(screen.queryByText(selectors.support.productionSla)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.support.professionalSla)).not.toBeInTheDocument();

            expect(screen.queryByText(selectors.support.upgradeYourSupport)).toBeInTheDocument();
            expect(screen.queryByText(selectors.support.upgradeToProduction)).not.toBeInTheDocument();
        });

        it("professional support - on premise", async function () {
            const { screen, fireClick } = rtlRender(<ProfessionalSupportOnPremise />);

            expect(await screen.findByText(selectors.licenseServer.connected)).toBeInTheDocument();

            await fireClick(screen.queryByText(selectors.support.supportPlanTab));

            expect(screen.queryByText(selectors.support.productionSla)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.support.professionalSla)).toBeInTheDocument();

            expect(screen.queryByText(selectors.support.upgradeYourSupport)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.support.upgradeToProduction)).toBeInTheDocument();
        });

        it("professional support - cloud", async function () {
            const { screen, fireClick } = rtlRender(<ProfessionalSupportCloud />);

            expect(await screen.findByText(selectors.licenseServer.connected)).toBeInTheDocument();

            await fireClick(screen.queryByText(selectors.support.supportPlanTab));

            expect(screen.queryByText(selectors.support.productionSla)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.support.professionalSla)).toBeInTheDocument();

            expect(screen.queryByText(selectors.support.upgradeYourSupport)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.support.upgradeToProduction)).toBeInTheDocument();
        });

        it("production support - on premise", async function () {
            const { screen, fireClick } = rtlRender(<ProductionSupportOnPremise />);

            expect(await screen.findByText(selectors.licenseServer.connected)).toBeInTheDocument();

            await fireClick(screen.queryByText(selectors.support.supportPlanTab));

            expect(screen.queryByText(selectors.support.productionSla)).toBeInTheDocument();
            expect(screen.queryByText(selectors.support.professionalSla)).not.toBeInTheDocument();

            expect(screen.queryByText(selectors.support.upgradeYourSupport)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.support.upgradeToProduction)).not.toBeInTheDocument();
        });

        it("production support - cloud", async function () {
            const { screen, fireClick } = rtlRender(<ProductionSupportCloud />);

            expect(await screen.findByText(selectors.licenseServer.connected)).toBeInTheDocument();

            await fireClick(screen.queryByText(selectors.support.supportPlanTab));

            expect(screen.queryByText(selectors.support.productionSla)).toBeInTheDocument();
            expect(screen.queryByText(selectors.support.professionalSla)).not.toBeInTheDocument();

            expect(screen.queryByText(selectors.support.upgradeYourSupport)).not.toBeInTheDocument();
            expect(screen.queryByText(selectors.support.upgradeToProduction)).not.toBeInTheDocument();
        });
    });

    it("can retest connection to license server", async () => {
        const { screen } = rtlRender(<ConnectionFailure />);

        expect(await screen.findByText(selectors.licenseServer.retestButton)).toBeInTheDocument();
        expect(screen.queryAllByText(selectors.licenseServer.failure)).toHaveLength(2);
    });
});

const selectors = {
    licenseServer: {
        failure: /Unable to reach the RavenDB License Server/,
        retestButton: /Test again/,
        connected: /Connected/,
    },
    versions: {
        usingLatest: /You are using the latest version/,
        whatsNew: /What's New\?/,
        updateInstructions: /Update instructions/,
        close: "Close",
        changelog: /Changelog/,
    },
    agplLicense: /No license - AGPLv3 Restrictions/,
    licenseId: /License ID/,
    renewLicense: /Renew license/,
    registerLicense: /Register license/,
    forceUpdate: /Force Update/,
    replaceLicense: "Replace",

    support: {
        supportPlanTab: /Support plan/,
        productionSla: "2 hour SLA",
        professionalSla: "Next business day SLA",
        upgradeYourSupport: /Upgrade Your Support/,
        upgradeToProduction: /Upgrade to Production/,
    },
};
