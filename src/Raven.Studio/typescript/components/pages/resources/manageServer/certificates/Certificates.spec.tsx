import * as stories from "./Certificates.stories";
import { rtlRender, rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import { composeStories } from "@storybook/react-webpack5";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";
import moment from "moment";
import { waitFor } from "@testing-library/react";
import { mockServices } from "test/mocks/services/MockServices";
import notificationCenter from "common/notifications/notificationCenter";

const { CertificatesStory } = composeStories(stories);

const selectors = {
    authIsDisabledHeader: /Authentication is disabled/,
    renewNowButton: /Renew now/,
    renewConfirmButton: /Renew certificate/,
    readyDialogTitle: /Your new certificate is ready/,
    manageUserAccessButton: /Manage user access/,
    generateClientCertItem: /Generate client certificate/,
    nameInput: /Name/,
    generateCertButton: /Generate certificate/,
    saveAnywayButton: /Save anyway/,
    browserAccessTab: /Browser Access/,
    apiAccessTab: /Application Access \(API\)/,
    renewalDate: "2025-01-15",
    editButtonTitle: /Edit certificate/,
    deleteButtonTitle: /Delete certificate/,
    wellKnownServerCerts: /Well known admin certificates/,
    wellKnownIssuerCerts: /Well known issuer certificates/,
    stateFilterLabel: "Filter by state",
    expiredStateOption: "Expired",
    disabledStateOption: "Disabled",
};

const stubCertificates = ManageServerStubs.certificates().Certificates;
const clientCertName = stubCertificates[1].Name;
const aboutToExpireCertName = stubCertificates[2].Name;
const expiredCertName = stubCertificates[3].Name;

const stubSsoCertificates = ManageServerStubs.certificatesWithSso().Certificates;
const ssoUserCertName = stubSsoCertificates.find((x) => x.Usage === "SsoClient" && !x.Disabled).Name;
const disabledSsoUserCertName = stubSsoCertificates.find((x) => x.Usage === "SsoClient" && x.Disabled).Name;

describe("Certificates", () => {
    it("can render when server is not secure", () => {
        const { screen } = rtlRender(<CertificatesStory isSecureServer={false} />);

        expect(screen.getByRole("heading", { name: selectors.authIsDisabledHeader })).toBeInTheDocument();
    });

    it("can render when server is secure", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<CertificatesStory isSecureServer={true} />);

        expect(screen.queryByRole("heading", { name: selectors.authIsDisabledHeader })).not.toBeInTheDocument();
        expect(screen.getByRole("button", { name: /Manage user access/ })).toBeInTheDocument();
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

        it("does not open the certificate ready dialog after renewing the server certificate", async () => {
            const forceRenew = mockServices.manageServerService.getMock("forceRenewServerCertificate");
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(
                <CertificatesStory serverCertRenewalDate={selectors.renewalDate} serverCertSetupMode="LetsEncrypt" />
            );

            const renewButton = screen.getByRole("button", { name: selectors.renewNowButton });
            await fireClick(renewButton);

            const confirmButton = await screen.findByRole("button", { name: selectors.renewConfirmButton });
            await fireClick(confirmButton);

            await waitFor(() => expect(forceRenew).toHaveBeenCalled());
            expect(screen.queryByText(selectors.readyDialogTitle)).not.toBeInTheDocument();
        });
    });

    describe("client certificate", () => {
        afterEach(() => {
            jest.restoreAllMocks();
        });

        it("opens the certificate ready dialog after generating a client certificate", async () => {
            jest.spyOn(HTMLFormElement.prototype, "submit").mockImplementation(() => {});
            jest.spyOn(notificationCenter.instance, "monitorOperation").mockReturnValue($.Deferred<void>().resolve());
            const { screen, fireClick, user } = await rtlRender_WithWaitForLoad(<CertificatesStory />);

            await fireClick(screen.getByRole("button", { name: selectors.manageUserAccessButton }));
            await fireClick(await screen.findByText(selectors.generateClientCertItem));
            await user.type(await screen.findByRole("textbox", { name: selectors.nameInput }), "new-client");
            await fireClick(screen.getByRole("button", { name: selectors.generateCertButton }));
            await fireClick(await screen.findByRole("button", { name: selectors.saveAnywayButton }));

            expect(await screen.findByText(selectors.readyDialogTitle)).toBeInTheDocument();
            expect(screen.getByText(selectors.browserAccessTab)).toBeInTheDocument();
            expect(screen.getByText(selectors.apiAccessTab)).toBeInTheDocument();
        });

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
            const { screen, user } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={(x) => {
                        x.Certificates = [x.Certificates[0], x.Certificates[1]];
                        x.Certificates[1].NotAfter = moment().subtract(1, "days").format();
                    }}
                />
            );

            await selectStateFilterOption(screen, user, selectors.expiredStateOption);

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

    describe("sso user entry", () => {
        it("shows the Disabled status for a disabled SSO user which has no expiration date", async () => {
            const { screen, user } = await rtlRender_WithWaitForLoad(<CertificatesStory />);

            await selectStateFilterOption(screen, user, selectors.disabledStateOption);

            expect(getItemStatus(screen, disabledSsoUserCertName)).toHaveTextContent("Disabled");
        });

        it("shows the Valid status for an enabled SSO user which has no expiration date", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(
                <CertificatesStory
                    certificates={certificatesWithSso((x) => {
                        x.Certificates.find((cert) => cert.Name === ssoUserCertName).NotAfter = null;
                    })}
                />
            );

            expect(getItemStatus(screen, ssoUserCertName)).toHaveTextContent("Valid");
        });

        it("hides a disabled SSO user when only the Valid state is selected", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(<CertificatesStory />);

            expect(screen.getByText(ssoUserCertName)).toBeInTheDocument();
            expect(screen.queryByText(disabledSsoUserCertName)).not.toBeInTheDocument();
        });
    });

    describe("state filter", () => {
        it("shows only valid and about to expire certificates by default", async () => {
            const { screen } = await rtlRender_WithWaitForLoad(<CertificatesStory />);

            const stateFilter = screen.getByText(selectors.stateFilterLabel).closest("div");
            expect(stateFilter.querySelectorAll(".react-select__multi-value")).toHaveLength(1);
            expect(stateFilter.querySelector(".react-select__multi-value")).toHaveTextContent("Valid");

            expect(screen.getByText(clientCertName)).toBeInTheDocument();
            expect(screen.getByText(aboutToExpireCertName)).toBeInTheDocument();
            expect(screen.queryByText(expiredCertName)).not.toBeInTheDocument();
        });

        it("can show expired certificates after selecting the Expired state", async () => {
            const { screen, user } = await rtlRender_WithWaitForLoad(<CertificatesStory />);

            await selectStateFilterOption(screen, user, selectors.expiredStateOption);

            expect(screen.getByText(expiredCertName)).toBeInTheDocument();
        });
    });
});

function certificatesWithSso(mutate: (dto: CertificatesResponseDto) => void): CertificatesResponseDto {
    const dto = ManageServerStubs.certificatesWithSso();
    mutate(dto);
    return dto;
}

function getItemStatus(screen: ReturnType<typeof rtlRender>["screen"], certName: string) {
    return screen.getByText(certName).closest(".rich-panel-item").querySelector(".rich-panel-status");
}

async function selectStateFilterOption(
    screen: ReturnType<typeof rtlRender>["screen"],
    user: ReturnType<typeof rtlRender>["user"],
    optionLabel: string
) {
    const stateFilterInput = screen.getByText(selectors.stateFilterLabel).closest("div").querySelector("input");

    await user.click(stateFilterInput);
    await user.click(await screen.findByText(optionLabel));
}
