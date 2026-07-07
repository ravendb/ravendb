import { useState } from "react";
import Tab from "react-bootstrap/Tab";
import Nav from "react-bootstrap/Nav";
import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import { NumberedList, NumberedListItem } from "components/common/NumberedList";
import genUtils from "common/generalUtils";

export default function CertificatesRenewedModalBrowserAccess() {
    const [browserTab, setBrowserTab] = useState<Browser>(genUtils.getBrowser());

    return (
        <Tab.Pane eventKey="browser">
            <NumberedList>
                <NumberedListItem stepKey={1}>
                    <h4 className="mb-1">Locate the certificate file</h4>
                    <p className="mb-0">
                        Find the downloaded <code>.pfx</code> file in your computer&apos;s &quot;Downloads&quot; folder.
                        This file is a secure, password-protected package containing your new digital ID.
                    </p>
                </NumberedListItem>
                <NumberedListItem stepKey={2}>
                    <h4 className="mb-2">Install the certificate in your browser</h4>
                    <Tab.Container activeKey={browserTab} onSelect={(key: Browser) => setBrowserTab(key)}>
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
                                    Chrome (or any Chromium-based browser) will let you select this certificate
                                    automatically. You may need to restart all instances of Chrome to make sure nothing
                                    is cached.
                                </Tab.Pane>
                                <Tab.Pane eventKey="Firefox">
                                    Firefox uses its own internal certificate store. After importing the certificate
                                    through Firefox settings, it will be available for use automatically. You may need
                                    to restart Firefox to ensure the new certificate is recognized properly.
                                </Tab.Pane>
                                <Tab.Pane eventKey="Safari">
                                    Safari uses the macOS Keychain to manage certificates. Once the certificate is
                                    imported and marked as trusted in Keychain Access, Safari will select it
                                    automatically when needed. Restarting Safari or the system may help if it
                                    doesn&apos;t appear right away.
                                </Tab.Pane>
                                <Tab.Pane eventKey="Other">
                                    Browsers that are not Chromium-based and don&apos;t use the system certificate store
                                    typically require manual certificate import through their own settings or
                                    preferences. Behavior may vary, and restarting the browser is often recommended to
                                    ensure the certificate is applied.
                                </Tab.Pane>
                            </Tab.Content>
                        </div>
                    </Tab.Container>
                </NumberedListItem>
                <NumberedListItem stepKey={3}>
                    <h4 className="mb-1">Reconnect to Studio</h4>
                    <p className="mb-1">To activate the new certificate, you may need to restart your browser.</p>
                    <ul className="mb-0">
                        <li>Close all browser windows completely.</li>
                        <li>
                            Reopen the Studio URL. Your browser should now prompt you to select the new certificate you
                            just installed.
                        </li>
                    </ul>
                </NumberedListItem>
            </NumberedList>
            <RichAlert variant="info">
                If you&apos;re not prompted to choose a certificate, try using an Incognito/Private window.
            </RichAlert>
        </Tab.Pane>
    );
}
