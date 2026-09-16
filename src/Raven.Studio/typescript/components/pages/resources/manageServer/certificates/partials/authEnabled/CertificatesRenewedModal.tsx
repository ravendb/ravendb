import { useState } from "react";
import Modal from "components/common/Modal";
import Tab from "react-bootstrap/Tab";
import Nav from "react-bootstrap/Nav";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppDispatch } from "components/store";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import CertificatesRenewedModalBrowserAccess from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesRenewedModalBrowserAccess";
import CertificatesRenewedModalApiAccess from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesRenewedModalApiAccess";
import "./CertificatesRenewedModal.scss";

type AccessTab = "browser" | "api";

export default function CertificatesRenewedModal() {
    const dispatch = useAppDispatch();
    const docsLink = useRavenLink({ hash: "RSFSL5" });

    const [accessTab, setAccessTab] = useState<AccessTab>("browser");

    const close = () => dispatch(certificatesActions.isRenewedModalOpenToggled());

    return (
        <Modal
            scrollable
            show
            size="lg"
            centered
            contentClassName="modal-border bulge-primary certificates-renewed-modal"
        >
            <Modal.Header onCloseClick={close} className="mb-0">
                <Icon icon="certificate" size="lg" margin="me-3" color="primary" addon="check" />
                <h3 className="mb-0">Your new certificate is ready</h3>
            </Modal.Header>
            <Modal.Body>
                <p className="mb-4">
                    Your new certificate has been created and downloaded to your computer. To begin using it, you need
                    to install it or move it to a secure location for your application. Choose the option below that
                    matches how you&apos;ll connect to RavenDB.
                </p>

                <Tab.Container activeKey={accessTab} onSelect={(key: AccessTab) => setAccessTab(key)}>
                    <div className="access-tab-container mb-4">
                        <Nav className="gap-3">
                            <Nav.Item className="flex-grow mb-0">
                                <Nav.Link
                                    eventKey="browser"
                                    className="d-flex align-items-center justify-content-center w-100 text-center border rounded py-2 px-3"
                                >
                                    <Icon icon="global" />
                                    Browser Access
                                </Nav.Link>
                            </Nav.Item>
                            <Nav.Item className="flex-grow mb-0">
                                <Nav.Link
                                    eventKey="api"
                                    className="d-flex align-items-center justify-content-center w-100 text-center border rounded py-2 px-3"
                                >
                                    <Icon icon="code" />
                                    Application Access (API)
                                </Nav.Link>
                            </Nav.Item>
                        </Nav>
                    </div>

                    <Tab.Content>
                        <CertificatesRenewedModalBrowserAccess />
                        <CertificatesRenewedModalApiAccess />
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
