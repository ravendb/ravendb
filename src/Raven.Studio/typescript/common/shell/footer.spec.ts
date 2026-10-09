/**
 * @jest-environment jsdom
 * @jest-environment-options {"url": "https://a-my-cluster.sso.example.com/studio/index.html"}
 */

import footer from "common/shell/footer";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import clientCertificateModel from "models/auth/clientCertificateModel";
import { ClusterStubs } from "test/stubs/ClusterStubs";
import CertificateUsage = Raven.Client.ServerWide.Operations.Certificates.CertificateUsage;
import CertificateDefinition = Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition;

function loginWith(usage: CertificateUsage) {
    clientCertificateModel.certificateInfo({ Usage: usage } as CertificateDefinition & {
        HasTwoFactor: boolean;
        TwoFactorExpirationDate: string;
    });
}

function nodeB() {
    return ClusterStubs.clusterTopology()
        .nodes()
        .find((node) => node.tag() === "B");
}

describe("footer", () => {
    beforeEach(() => {
        clusterTopologyManager.default.topology(ClusterStubs.clusterTopology());
    });

    afterEach(() => {
        clientCertificateModel.certificateInfo(null);
        clusterTopologyManager.default.topology(ClusterStubs.singleNodeTopology());
    });

    it("links another node through its SSO proxy host when logged in via SSO", () => {
        loginWith("SsoClient");

        expect(footer.default.nodeServerUrl(nodeB())()).toBe("https://b-my-cluster.sso.example.com");
    });

    it("builds the SSO node host from the cluster alias when Studio is opened on the cluster host", () => {
        expect(footer.ssoNodeHost("my-cluster.sso.example.com", "B", "A")).toBe("a-my-cluster.sso.example.com");
    });

    it("links another node directly when logged in with a client certificate", () => {
        loginWith("Client");

        expect(footer.default.nodeServerUrl(nodeB())()).toBe("http://raven2:8080");
    });
});
