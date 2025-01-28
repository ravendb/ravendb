import * as stories from "./Certificates.stories";
import { rtlRender, rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import { composeStories } from "@storybook/react";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";
import moment from "moment";

const { CertificatesStory } = composeStories(stories);

const selectors = {
    authIsDisabledHeader: /Authentication is disabled/,
    renewNowButton: /Renew now/,
    renewalDate: "2025-01-15",
    regenerateButton: /Regenerate/,
    editButtonTitle: /Edit certificate/,
    deleteButtonTitle: /Delete certificate/,
    wellKnownServerCerts: /Well known admin certificates/,
    wellKnownIssuerCerts: /Well known issuer certificates/,
};

const serverCert = ManageServerStubs.certificates().Certificates[0];
const clientCert = ManageServerStubs.certificates().Certificates[1];

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
    });

    describe("client certificate", () => {
        it("can show regenerate button when cert is about to expire", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.Certificates = [serverCert, clientCert];
                        x.Certificates[1].NotAfter = moment().add(14, "days").format();
                    }}
                />
            );

            expect(screen.getByRole("button", { name: selectors.regenerateButton })).toBeInTheDocument();
        });

        it("can hide regenerate button when cert is valid", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.Certificates = [serverCert, clientCert];
                        x.Certificates[1].NotAfter = moment().add(15, "days").format();
                    }}
                />
            );

            expect(screen.queryByRole("button", { name: selectors.regenerateButton })).not.toBeInTheDocument();
        });

        it("can show edit button when cert is not expired", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.Certificates = [serverCert, clientCert];
                        x.Certificates[1].NotAfter = moment().add(1, "days").format();
                    }}
                />
            );

            expect(screen.getByTitle(selectors.editButtonTitle)).toBeInTheDocument();
        });

        it("can hide edit button when cert is expired", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.Certificates = [serverCert, clientCert];
                        x.Certificates[1].NotAfter = moment().subtract(1, "days").format();
                    }}
                />
            );

            expect(screen.queryByTitle(selectors.editButtonTitle)).not.toBeInTheDocument();
        });

        it("can show delete button when clearance is not ClusterAdmin", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.Certificates = [serverCert, clientCert];
                        x.Certificates[1].SecurityClearance = "Operator";
                    }}
                />
            );

            expect(screen.getByTitle(selectors.deleteButtonTitle)).toBeInTheDocument();
        });

        it("can hide delete button when clearance is ClusterAdmin", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.Certificates = [serverCert, clientCert];
                        x.Certificates[1].SecurityClearance = "ClusterAdmin";
                    }}
                />
            );

            expect(screen.queryByTitle(selectors.deleteButtonTitle)).not.toBeInTheDocument();
        });
    });
});
