import * as stories from "./Certificates.stories";
import { rtlRender, rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import { composeStories } from "@storybook/react-webpack5";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";
import moment from "moment";

const { CertificatesStory } = composeStories(stories);

const selectors = {
    authIsDisabledHeader: /Authentication is disabled/,
    renewNowButton: /Renew now/,
    renewConfirmButton: /Renew certificate/,
    renewedDialogTitle: /Your new certificate is ready/,
    browserAccessTab: /Browser Access/,
    apiAccessTab: /Application Access \(API\)/,
    renewalDate: "2025-01-15",
    editButtonTitle: /Edit certificate/,
    deleteButtonTitle: /Delete certificate/,
    wellKnownServerCerts: /Well known admin certificates/,
    wellKnownIssuerCerts: /Well known issuer certificates/,
    validStateFilter: /Valid/,
    expiredStateFilter: /Expired/,
};

const stubCertificates = ManageServerStubs.certificates().Certificates;
const clientCertName = stubCertificates[1].Name;
const aboutToExpireCertName = stubCertificates[2].Name;
const expiredCertName = stubCertificates[3].Name;

describe("Certificates", () => {
    it("can render when server is not secure", () => {
        const { screen } = rtlRender(<CertificatesStory isSecureServer={false} />);

        expect(screen.getByRole("heading", { name: selectors.authIsDisabledHeader })).toBeInTheDocument();
    });

    it("can render when server is secure", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<CertificatesStory isSecureServer={true} />);

        expect(screen.queryByRole("heading", { name: selectors.authIsDisabledHeader })).not.toBeInTheDocument();
        expect(screen.getByRole("button", { name: /Manage certificates/ })).toBeInTheDocument();
    });

    describe("well known certs", () => {
        it("can show well known server certs", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.WellKnownAdminCerts = ["some-thumbprint"];
                    }}
                />
            );

            expect(screen.getByText(selectors.wellKnownServerCerts)).toBeInTheDocument();
            expect(screen.getByText(/some-thumbprint/)).toBeInTheDocument();
        });

        it("can hide well known server certs", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.WellKnownAdminCerts = [];
                    }}
                />
            );

            expect(screen.queryByText(selectors.wellKnownServerCerts)).not.toBeInTheDocument();
        });

        it("can show well known issuers certs", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.WellKnownIssuers = ["some-thumbprint"];
                    }}
                />
            );

            expect(screen.getByText(selectors.wellKnownIssuerCerts)).toBeInTheDocument();
            expect(screen.getByText(/some-thumbprint/)).toBeInTheDocument();
        });

        it("can hide well known issuers certs", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.WellKnownIssuers = [];
                    }}
                />
            );

            expect(screen.queryByText(selectors.wellKnownIssuerCerts)).not.toBeInTheDocument();
        });
    });

    describe("server certificate", () => {
        it("can show renew now when setup mode is LetsEncrypt", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory serverCertRenewalDate={selectors.renewalDate} serverCertSetupMode="LetsEncrypt" />
            );

            expect(screen.getByText(selectors.renewalDate)).toBeInTheDocument();
            expect(screen.getByRole("button", { name: selectors.renewNowButton })).toBeInTheDocument();
        });

        it("can hide renew now when setup mode is not LetsEncrypt", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory serverCertRenewalDate={selectors.renewalDate} serverCertSetupMode="None" />
            );

            expect(screen.queryByText(selectors.renewalDate)).not.toBeInTheDocument();
            expect(screen.queryByRole("button", { name: selectors.renewNowButton })).not.toBeInTheDocument();
        });

        it("opens the guidance dialog after renewing the server certificate", async () => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(
                <CertificatesStory serverCertRenewalDate={selectors.renewalDate} serverCertSetupMode="LetsEncrypt" />
            );

            const renewButton = screen.getByRole("button", { name: selectors.renewNowButton });
            await fireClick(renewButton);

            const confirmButton = await screen.findByRole("button", { name: selectors.renewConfirmButton });
            await fireClick(confirmButton);

            expect(await screen.findByText(selectors.renewedDialogTitle)).toBeInTheDocument();
            expect(screen.getByText(selectors.browserAccessTab)).toBeInTheDocument();
            expect(screen.getByText(selectors.apiAccessTab)).toBeInTheDocument();
        });
    });

    describe("client certificate", () => {
        it("can show edit button when cert is not expired", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.Certificates = [x.Certificates[0], x.Certificates[1]];
                        x.Certificates[1].NotAfter = moment().add(1, "days").format();
                    }}
                />
            );

            expect(screen.getByTitle(selectors.editButtonTitle)).toBeInTheDocument();
        });

        it("can hide edit button when cert is expired", async () => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.Certificates = [x.Certificates[0], x.Certificates[1]];
                        x.Certificates[1].NotAfter = moment().subtract(1, "days").format();
                    }}
                />
            );

            await fireClick(screen.getByLabelText(selectors.expiredStateFilter));

            expect(screen.getByText(clientCertName)).toBeInTheDocument();
            expect(screen.queryByTitle(selectors.editButtonTitle)).not.toBeInTheDocument();
        });

        it("can show delete button when clearance is not ClusterAdmin", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.Certificates = [x.Certificates[0], x.Certificates[1]];
                        x.Certificates[1].SecurityClearance = "Operator";
                    }}
                    securityClearance="Operator"
                />
            );

            expect(screen.getByTitle(selectors.deleteButtonTitle)).toBeInTheDocument();
        });

        it("can hide delete button when clearance is ClusterAdmin", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.Certificates = [x.Certificates[0], x.Certificates[1]];
                        x.Certificates[1].SecurityClearance = "ClusterAdmin";
                    }}
                    securityClearance="Operator"
                />
            );

            expect(screen.queryByTitle(selectors.deleteButtonTitle)).not.toBeInTheDocument();
        });
    });

    describe("state filter", () => {
        it("shows only valid and about to expire certificates by default", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(<CertificatesStory />);

            expect(screen.getByLabelText(selectors.validStateFilter)).toBeChecked();
            expect(screen.getByLabelText(selectors.expiredStateFilter)).not.toBeChecked();

            expect(screen.getByText(clientCertName)).toBeInTheDocument();
            expect(screen.getByText(aboutToExpireCertName)).toBeInTheDocument();
            expect(screen.queryByText(expiredCertName)).not.toBeInTheDocument();
        });

        it("can show expired certificates after selecting the Expired state", async () => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(<CertificatesStory />);

            await fireClick(screen.getByLabelText(selectors.expiredStateFilter));

            expect(screen.getByText(expiredCertName)).toBeInTheDocument();
        });
    });
});
