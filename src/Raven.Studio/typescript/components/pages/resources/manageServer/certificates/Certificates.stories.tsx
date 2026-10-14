import { Meta, StoryObj } from "@storybook/react-webpack5";
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
    additionalDatabasePermissions: number;
}

const accessLevels: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess[] = ["Read", "ReadWrite", "Admin"];

function withAdditionalDatabasePermissions(
    certificates: MockedValue<CertificatesResponseDto>,
    count: number
): MockedValue<CertificatesResponseDto> {
    if (count === 0 || typeof certificates === "function") {
        return certificates;
    }

    const additionalPermissions = Object.fromEntries(
        Array.from({ length: count }, (_, i) => [`customer-database-${i + 1}`, accessLevels[i % accessLevels.length]])
    );

    return {
        ...certificates,
        Certificates: certificates.Certificates.map((certificate) =>
            certificate.SecurityClearance === "ValidUser" && certificate.Usage !== "SsoServer"
                ? { ...certificate, Permissions: { ...certificate.Permissions, ...additionalPermissions } }
                : certificate
        ),
    };
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
        manageServerService.withCertificates(
            withAdditionalDatabasePermissions(args.certificates, args.additionalDatabasePermissions)
        );

        return <Certificates />;
    },
    args: {
        securityClearance: "ClusterAdmin",
        isSecureServer: true,
        hasReadOnlyCertificates: true,
        certificates: ManageServerStubs.certificatesWithSso(),
        serverCertSetupMode: ManageServerStubs.serverCertificateSetupMode(),
        serverCertRenewalDate: ManageServerStubs.serverCertificateRenewalDate(),
        additionalDatabasePermissions: 0,
    },
    argTypes: {
        securityClearance: securityClearanceArgType,
        additionalDatabasePermissions: { control: { type: "range", min: 0, max: 100, step: 1 } },
    },
};
