import { Meta, StoryObj } from "@storybook/react/*";
import Certificates from "components/pages/resources/manageServer/certificates/Certificates";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";

export default {
    title: "Pages/ManageServer/Certificates",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface CertificatesStoryArgs {
    isSecureServer: boolean;
    hasReadOnlyCertificates: boolean;
}

export const CertificatesStory: StoryObj<CertificatesStoryArgs> = {
    name: "Certificates",
    render: (args) => {
        const { manageServerService } = mockServices;
        const { accessManager, databases, cluster, license } = mockStore;

        accessManager.with_isServerSecure(args.isSecureServer);
        accessManager.with_clientCertificateThumbprint(ManageServerStubs.certificates().Certificates[1].Thumbprint);
        databases.with_Single();
        cluster.with_Single();

        license.with_License({
            HasReadOnlyCertificates: args.hasReadOnlyCertificates,
        });

        manageServerService.withGenerateTwoFactorSecret();
        manageServerService.withAdminStats();
        manageServerService.withServerCertificateRenewalDate();
        manageServerService.withServerCertificateSetupMode();
        manageServerService.withCertificates();

        return <Certificates />;
    },
    args: {
        isSecureServer: true,
        hasReadOnlyCertificates: true,
    },
};
