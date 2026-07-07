import { Meta, StoryObj } from "@storybook/react/*";
import Certificates from "components/pages/resources/manageServer/certificates/Certificates";
import { MockedValue } from "test/mocks/services/AutoMockService";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import {
    withStorybookContexts,
    withBootstrap5,
    securityClearanceArgType,
    withForceRerender,
} from "test/storybookTestUtils";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";

export default {
    title: "Pages/Manage Server/Certificates",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface CertificatesStoryArgs {
    isSecureServer: boolean;
    hasReadOnlyCertificates: boolean;
    certificates: MockedValue<CertificatesResponseDto>;
    serverCertSetupMode: MockedValue<Raven.Server.Commercial.SetupMode>;
    serverCertRenewalDate: MockedValue<string>;
    securityClearance: securityClearance;
}

export const CertificatesStory: StoryObj<CertificatesStoryArgs> = {
    name: "Certificates",
    render: (args) => {
        const { manageServerService } = mockServices;
        const { accessManager, databases, cluster, license } = mockStore;

        accessManager.with_securityClearance(args.securityClearance);
        accessManager.with_isServerSecure(args.isSecureServer);
        accessManager.with_clientCertificateThumbprint(ManageServerStubs.certificates().Certificates[1].Thumbprint);
        databases.with_Single();
        cluster.with_Single();

        license.with_License({
            HasReadOnlyCertificates: args.hasReadOnlyCertificates,
        });

        manageServerService.withGenerateTwoFactorSecret();
        manageServerService.withAdminStats();
        manageServerService.withServerCertificateRenewalDate(args.serverCertRenewalDate);
        manageServerService.withServerCertificateSetupMode(args.serverCertSetupMode);
        manageServerService.withCertificates(args.certificates);

        return <Certificates />;
    },
    args: {
        securityClearance: "ClusterAdmin",
        isSecureServer: true,
        hasReadOnlyCertificates: true,
        certificates: ManageServerStubs.certificatesWithSso(),
        serverCertSetupMode: ManageServerStubs.serverCertificateSetupMode(),
        serverCertRenewalDate: ManageServerStubs.serverCertificateRenewalDate(),
    },
    argTypes: {
        securityClearance: securityClearanceArgType,
    },
};
