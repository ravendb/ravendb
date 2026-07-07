import { useState } from "react";
import Tab from "react-bootstrap/Tab";
import Nav from "react-bootstrap/Nav";
import { Icon } from "components/common/Icon";
import { NumberedList, NumberedListItem } from "components/common/NumberedList";
import Code, { CodeLanguage } from "components/common/Code";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

type ClientLanguage = Extract<CodeLanguage, "csharp" | "nodejs" | "python" | "java">;

const serverUrlPlaceholder = "https://your_RavenDB_server_URL";
const pfxPathPlaceholder = "C:\\path_to_your_pfx_file\\cert.pfx";
const dbNamePlaceholder = "your_database_name";

export default function CertificatesRenewedModalApiAccess() {
    const docsLink = useRavenLink({ hash: "RSFSL5" });

    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);
    const localNode = useAppSelector(clusterSelectors.localNode);

    const serverUrl = localNode?.serverUrl ?? serverUrlPlaceholder;
    const databaseName = dbName ?? dbNamePlaceholder;

    const [clientTab, setClientTab] = useState<ClientLanguage>("csharp");

    return (
        <Tab.Pane eventKey="api">
            <NumberedList>
                <NumberedListItem stepKey={1}>
                    <h4 className="mb-1">Locate and secure the certificate file</h4>
                    <p className="mb-0">
                        Find the automatically downloaded <code>.pfx</code> file in your computer&apos;s
                        &quot;Downloads&quot; folder and move it to a secure location accessible by your application
                        (e.g., a protected folder on your server or a cloud secrets vault).
                    </p>
                </NumberedListItem>
                <NumberedListItem stepKey={2}>
                    <h4 className="mb-2">Configure your RavenDB client code</h4>
                    <p className="mb-2">
                        Update your application&apos;s code to point the <code>DocumentStore</code> initialization to
                        the new certificate&apos;s file path and password.
                    </p>
                    <Tab.Container activeKey={clientTab} onSelect={(key: ClientLanguage) => setClientTab(key)}>
                        <div className="segmented-tab-container">
                            <Nav>
                                <Nav.Item className="flex-grow">
                                    <Nav.Link eventKey="csharp">C#</Nav.Link>
                                </Nav.Item>
                                <Nav.Item className="flex-grow">
                                    <Nav.Link eventKey="java">Java</Nav.Link>
                                </Nav.Item>
                                <Nav.Item className="flex-grow">
                                    <Nav.Link eventKey="nodejs">Node.js</Nav.Link>
                                </Nav.Item>
                                <Nav.Item className="flex-grow">
                                    <Nav.Link eventKey="python">Python</Nav.Link>
                                </Nav.Item>
                            </Nav>
                            <Tab.Content className="p-2">
                                <Tab.Pane eventKey="csharp">
                                    <Code
                                        language="csharp"
                                        code={csharpSnippet(serverUrl, databaseName)}
                                        elementToCopy={csharpSnippet(serverUrl, databaseName)}
                                    />
                                </Tab.Pane>
                                <Tab.Pane eventKey="java">
                                    <Code
                                        language="java"
                                        code={javaSnippet(serverUrl, databaseName)}
                                        elementToCopy={javaSnippet(serverUrl, databaseName)}
                                    />
                                </Tab.Pane>
                                <Tab.Pane eventKey="nodejs">
                                    <Code
                                        language="javascript"
                                        code={nodejsSnippet(serverUrl, databaseName)}
                                        elementToCopy={nodejsSnippet(serverUrl, databaseName)}
                                    />
                                </Tab.Pane>
                                <Tab.Pane eventKey="python">
                                    <Code
                                        language="python"
                                        code={pythonSnippet(serverUrl, databaseName)}
                                        elementToCopy={pythonSnippet(serverUrl, databaseName)}
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
    );
}

function csharpSnippet(serverUrl: string, databaseName: string): string {
    return `// Load an X.509 certificate
X509Certificate2 clientCertificate = new X509Certificate2("${pfxPathPlaceholder}");

using (IDocumentStore store = new DocumentStore()
{
    // Set the 'Certificate' property with your client certificate
    Certificate = clientCertificate,
    Database = "${databaseName}",
    Urls = new[] {"${serverUrl}"}
}.Initialize())
{
    // Do your work here
}`;
}

function javaSnippet(serverUrl: string, databaseName: string): string {
    return `// Load an X.509 certificate into a KeyStore
KeyStore clientStore = KeyStore.getInstance("PKCS12");
clientStore.load(new FileInputStream("${pfxPathPlaceholder}"), "your_password".toCharArray());

try (IDocumentStore store = new DocumentStore(
        new String[]{"${serverUrl}"},
        "${databaseName}",
        new KeyStoreOptions(clientStore, "your_password"))) {
    store.initialize();
    // Do your work here
}`;
}

function nodejsSnippet(serverUrl: string, databaseName: string): string {
    return `import * as fs from "fs";
import { DocumentStore } from "ravendb";

const authOptions = {
    certificate: fs.readFileSync("${pfxPathPlaceholder}"),
    type: "pfx",
    password: "your_password"
};

const store = new DocumentStore(["${serverUrl}"], "${databaseName}", authOptions);
store.initialize();
// Do your work here`;
}

function pythonSnippet(serverUrl: string, databaseName: string): string {
    return `from ravendb import DocumentStore

store = DocumentStore(
    urls=["${serverUrl}"],
    database="${databaseName}",
)
store.certificate_pem_path = "path_to_your_cert.pem"
store.initialize()
# Do your work here`;
}
