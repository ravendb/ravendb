import { useState } from "react";
import Modal from "components/common/Modal";
import Tab from "react-bootstrap/Tab";
import Nav from "react-bootstrap/Nav";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import { NumberedList, NumberedListItem } from "components/common/NumberedList";
import Code from "components/common/Code";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppDispatch, useAppSelector } from "components/store";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import genUtils from "common/generalUtils";
import "./CertificatesRenewedModal.scss";

const serverUrlPlaceholder = "https://your_RavenDB_server_URL";
const pfxPathPlaceholder = "C:\\path_to_your_pfx_file\\cert.pfx";
const dbNamePlaceholder = "your_database_name";

type ClientLanguage = "csharp" | "java" | "nodejs" | "python";

export default function CertificatesRenewedModal() {
    const dispatch = useAppDispatch();
    const docsLink = useRavenLink({ hash: "RSFSL5" });

    const localNode = useAppSelector(clusterSelectors.localNode);
    const serverUrl = localNode?.serverUrl || serverUrlPlaceholder;

    const [accessTab, setAccessTab] = useState<"browser" | "api">("browser");
    const [browserTab, setBrowserTab] = useState<Browser>(genUtils.getBrowser());
    const [clientTab, setClientTab] = useState<ClientLanguage>("csharp");

    const close = () => dispatch(certificatesActions.isRenewedModalOpenToggled());

    return (
        <Modal scrollable show size="lg" centered contentClassName="modal-border bulge-primary certificates-renewed-modal">
            <Modal.Header onCloseClick={close}>
                <Icon icon="certificate" color="primary" addon="check" className="fs-2" />
                <span className="lead">Your new certificate is ready</span>
            </Modal.Header>
            <Modal.Body>
                <p className="mb-4">
                    Your new certificate has been created and downloaded to your computer. To begin using it, you need
                    to install it or move it to a secure location for your application. Choose the option below that
                    matches how you&apos;ll connect to RavenDB.
                </p>

                <Tab.Container activeKey={accessTab} onSelect={(key: "browser" | "api") => setAccessTab(key)}>
                    <div className="access-tab-container mb-4">
                        <Nav>
                            <Nav.Item className="flex-grow mb-0">
                                <Nav.Link eventKey="browser" className="">
                                    <Icon icon="global" />
                                    Browser Access
                                </Nav.Link>
                            </Nav.Item>
                            <Nav.Item className="flex-grow mb-0">
                                <Nav.Link eventKey="api" className="">
                                    <Icon icon="code" />
                                    Application Access (API)
                                </Nav.Link>
                            </Nav.Item>
                        </Nav>
                    </div>

                    <Tab.Content>
                        <Tab.Pane eventKey="browser">
                            <NumberedList>
                                <NumberedListItem stepKey={1}>
                                    <h4 className="mb-1">Locate the certificate file</h4>
                                    <p className="mb-0">
                                        Find the downloaded <code>.pfx</code> file in your computer&apos;s
                                        &quot;Downloads&quot; folder. This file is a secure, password-protected package
                                        containing your new digital ID.
                                    </p>
                                </NumberedListItem>
                                <NumberedListItem stepKey={2}>
                                    <h4 className="mb-2">Install the certificate in your browser</h4>
                                    <Tab.Container
                                        activeKey={browserTab}
                                        onSelect={(key: Browser) => setBrowserTab(key)}
                                    >
                                        <div className="segmented-tab-container">
                                            <Nav>
                                                <Nav.Item className="flex-grow">
                                                    <Nav.Link eventKey="Chrome">
                                                        <Icon icon="chrome" />
                                                        Chrome
                                                    </Nav.Link>
                                                </Nav.Item>
                                                <Nav.Item className="flex-grow">
                                                    <Nav.Link eventKey="Firefox">
                                                        <Icon icon="firefox" />
                                                        Firefox
                                                    </Nav.Link>
                                                </Nav.Item>
                                                <Nav.Item className="flex-grow">
                                                    <Nav.Link eventKey="Safari">
                                                        <Icon icon="safari" />
                                                        Safari
                                                    </Nav.Link>
                                                </Nav.Item>
                                                <Nav.Item className="flex-grow">
                                                    <Nav.Link eventKey="Other">
                                                        <Icon icon="global" />
                                                        Other
                                                    </Nav.Link>
                                                </Nav.Item>
                                            </Nav>
                                            <Tab.Content className="p-2 text-break">
                                                <Tab.Pane eventKey="Chrome">
                                                    Chrome (or any Chromium-based browser) will let you select this
                                                    certificate automatically. You may need to restart all instances of
                                                    Chrome to make sure nothing is cached.
                                                </Tab.Pane>
                                                <Tab.Pane eventKey="Firefox">
                                                    Firefox uses its own internal certificate store. After importing the
                                                    certificate through Firefox settings, it will be available for use
                                                    automatically. You may need to restart Firefox to ensure the new
                                                    certificate is recognized properly.
                                                </Tab.Pane>
                                                <Tab.Pane eventKey="Safari">
                                                    Safari uses the macOS Keychain to manage certificates. Once the
                                                    certificate is imported and marked as trusted in Keychain Access,
                                                    Safari will select it automatically when needed. Restarting Safari or
                                                    the system may help if it doesn&apos;t appear right away.
                                                </Tab.Pane>
                                                <Tab.Pane eventKey="Other">
                                                    Browsers that are not Chromium-based and don&apos;t use the system
                                                    certificate store typically require manual certificate import through
                                                    their own settings or preferences. Behavior may vary, and restarting
                                                    the browser is often recommended to ensure the certificate is
                                                    applied.
                                                </Tab.Pane>
                                            </Tab.Content>
                                        </div>
                                    </Tab.Container>
                                </NumberedListItem>
                                <NumberedListItem stepKey={3}>
                                    <h4 className="mb-1">Reconnect to Studio</h4>
                                    <p className="mb-1">
                                        To activate the new certificate, you may need to restart your browser.
                                    </p>
                                    <ul className="mb-0">
                                        <li>Close all browser windows completely.</li>
                                        <li>
                                            Reopen the Studio URL. Your browser should now prompt you to select the new
                                            certificate you just installed.
                                        </li>
                                    </ul>
                                </NumberedListItem>
                            </NumberedList>
                            <RichAlert variant="info">
                                If you&apos;re not prompted to choose a certificate, try using an Incognito/Private
                                window.
                            </RichAlert>
                        </Tab.Pane>

                        <Tab.Pane eventKey="api">
                            <NumberedList>
                                <NumberedListItem stepKey={1}>
                                    <h4 className="mb-1">Locate and secure the certificate file</h4>
                                    <p className="mb-0">
                                        Find the automatically downloaded <code>.pfx</code> file in your computer&apos;s
                                        &quot;Downloads&quot; folder and move it to a secure location accessible by your
                                        application (e.g., a protected folder on your server or a cloud secrets vault).
                                    </p>
                                </NumberedListItem>
                                <NumberedListItem stepKey={2}>
                                    <h4 className="mb-2">Configure your RavenDB client code</h4>
                                    <p className="mb-2">
                                        Update your application&apos;s code to point the <code>DocumentStore</code>{" "}
                                        initialization to the new certificate&apos;s file path and password.
                                    </p>
                                    <Tab.Container
                                        activeKey={clientTab}
                                        onSelect={(key: ClientLanguage) => setClientTab(key)}
                                    >
                                        <div className="segmented-tab-container">
                                            <Nav>
                                                <Nav.Item className="flex-grow">
                                                    <Nav.Link eventKey="csharp">
                                                        <Icon icon="csharp" />
                                                        C#
                                                    </Nav.Link>
                                                </Nav.Item>
                                                <Nav.Item className="flex-grow">
                                                    <Nav.Link eventKey="java">
                                                        <Icon icon="code" />
                                                        Java
                                                    </Nav.Link>
                                                </Nav.Item>
                                                <Nav.Item className="flex-grow">
                                                    <Nav.Link eventKey="nodejs">
                                                        <Icon icon="node" />
                                                        Node.js
                                                    </Nav.Link>
                                                </Nav.Item>
                                                <Nav.Item className="flex-grow">
                                                    <Nav.Link eventKey="python">
                                                        <Icon icon="code" />
                                                        Python
                                                    </Nav.Link>
                                                </Nav.Item>
                                            </Nav>
                                            <Tab.Content className="p-2">
                                                <Tab.Pane eventKey="csharp">
                                                    <Code
                                                        language="csharp"
                                                        code={csharpSnippet(serverUrl)}
                                                        elementToCopy={csharpSnippet(serverUrl)}
                                                    />
                                                </Tab.Pane>
                                                <Tab.Pane eventKey="java">
                                                    <Code
                                                        language="java"
                                                        code={javaSnippet(serverUrl)}
                                                        elementToCopy={javaSnippet(serverUrl)}
                                                    />
                                                </Tab.Pane>
                                                <Tab.Pane eventKey="nodejs">
                                                    <Code
                                                        language="javascript"
                                                        code={nodejsSnippet(serverUrl)}
                                                        elementToCopy={nodejsSnippet(serverUrl)}
                                                    />
                                                </Tab.Pane>
                                                <Tab.Pane eventKey="python">
                                                    <Code
                                                        language="python"
                                                        code={pythonSnippet(serverUrl)}
                                                        elementToCopy={pythonSnippet(serverUrl)}
                                                    />
                                                </Tab.Pane>
                                            </Tab.Content>
                                        </div>
                                    </Tab.Container>
                                    <p className="mt-2 mb-0">
                                        For detailed code examples and more information, please refer to the official{" "}
                                        <a href={docsLink} target="_blank">
                                            Documentation <Icon icon="newtab" margin="m-0" />
                                        </a>
                                        .
                                    </p>
                                </NumberedListItem>
                                <NumberedListItem stepKey={3}>
                                    <h4 className="mb-1">Restart your application</h4>
                                    <p className="mb-0">
                                        Your application must be restarted to load the new certificate file into memory.
                                    </p>
                                </NumberedListItem>
                            </NumberedList>
                        </Tab.Pane>
                    </Tab.Content>
                </Tab.Container>
            </Modal.Body>
            <Modal.Footer className="hstack justify-content-between my-2">
                <a href={docsLink} target="_blank" className="btn btn-info rounded-pill">
                    See documentation <Icon icon="newtab" margin="m-0" />
                </a>
                <Button variant="link" onClick={close} className="link-muted">
                    Close
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

function csharpSnippet(serverUrl: string): string {
    return `// Load an X.509 certificate
X509Certificate2 clientCertificate = new X509Certificate2("${pfxPathPlaceholder}");

using (IDocumentStore store = new DocumentStore()
{
    // Set the 'Certificate' property with your client certificate
    Certificate = clientCertificate,
    Database = "${dbNamePlaceholder}",
    Urls = new[] {"${serverUrl}"}
}.Initialize())
{
    // Do your work here
}`;
}

function javaSnippet(serverUrl: string): string {
    return `// Load an X.509 certificate into a KeyStore
KeyStore clientStore = KeyStore.getInstance("PKCS12");
clientStore.load(new FileInputStream("${pfxPathPlaceholder}"), "your_password".toCharArray());

try (IDocumentStore store = new DocumentStore(
        new String[]{"${serverUrl}"},
        "${dbNamePlaceholder}",
        new KeyStoreOptions(clientStore, "your_password"))) {
    store.initialize();
    // Do your work here
}`;
}

function nodejsSnippet(serverUrl: string): string {
    return `import * as fs from "fs";
import { DocumentStore } from "ravendb";

const authOptions = {
    certificate: fs.readFileSync("${pfxPathPlaceholder}"),
    type: "pfx",
    password: "your_password"
};

const store = new DocumentStore(["${serverUrl}"], "${dbNamePlaceholder}", authOptions);
store.initialize();
// Do your work here`;
}

function pythonSnippet(serverUrl: string): string {
    return `from ravendb import DocumentStore

store = DocumentStore(
    urls=["${serverUrl}"],
    database="${dbNamePlaceholder}",
)
store.certificate_pem_path = "path_to_your_cert.pem"
store.initialize()
# Do your work here`;
}
