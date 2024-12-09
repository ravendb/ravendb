import { Meta, StoryObj } from "@storybook/react/*";
import Certificates from "components/pages/resources/manageServer/certificates/Certificates";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";

export default {
    title: "Pages/ManageServer/Certificates",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface CertificatesStoryArgs {
    isSecureServer: boolean;
}

export const CertificatesStory: StoryObj<CertificatesStoryArgs> = {
    name: "Certificates",
    render: (args) => {
        const { manageServerService } = mockServices;
        const { accessManager } = mockStore;

        accessManager.with_isServerSecure(args.isSecureServer);

        manageServerService.withAdminStats();
        manageServerService.withCertificates();

        return <Certificates />;
    },
    args: {
        isSecureServer: true,
    },
};
