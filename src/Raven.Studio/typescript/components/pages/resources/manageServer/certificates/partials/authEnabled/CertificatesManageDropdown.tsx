import { Icon } from "components/common/Icon";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { useAppDispatch, useAppSelector } from "components/store";
import { useRef } from "react";
import Dropdown from "react-bootstrap/Dropdown";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useEventsCollector } from "components/hooks/useEventsCollector";

export default function CertificatesManageDropdown() {
    const dispatch = useAppDispatch();
    const { reportEvent } = useEventsCollector();
    const exportServerCertFormRef = useRef<HTMLFormElement>(null);
    const hasClusterNodeCertificate = useAppSelector(certificatesSelectors.hasClusterNodeCertificate);

    return (
        <Dropdown>
            <Dropdown.Toggle title="Manage certificates" variant="primary" className="rounded-pill">
                Manage certificates
            </Dropdown.Toggle>
            <Dropdown.Menu>
                <Dropdown.Header className="small-label">Client</Dropdown.Header>
                <Dropdown.Item onClick={() => dispatch(certificatesActions.isGenerateModalOpenToggled())}>
                    <Icon icon="certificate" addon="plus" />
                    Generate client certificate
                </Dropdown.Item>
                <Dropdown.Item onClick={() => dispatch(certificatesActions.isUploadModalOpenToggled())}>
                    <Icon icon="upload" />
                    Upload client certificate
                </Dropdown.Item>
                <Dropdown.Divider />
                <Dropdown.Header className="small-label">Server</Dropdown.Header>
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: !hasClusterNodeCertificate,
                            message: "You need to have a server certificate to export it",
                        },
                        {
                            isActive: true,
                            message: (
                                <span>
                                    Export the server certificate(s) without their private key into a .pfx file. These
                                    certificates can be used during a manual cluster setup, when you need to register
                                    server certificates to be trusted on other nodes.
                                </span>
                            ),
                        },
                    ]}
                    popoverPlacement="right"
                >
                    <Dropdown.Item
                        onClick={() => {
                            reportEvent("certificates", "export-certs");
                            exportServerCertFormRef.current?.submit();
                        }}
                        disabled={!hasClusterNodeCertificate}
                    >
                        <Icon icon="download" />
                        Export server certificate
                    </Dropdown.Item>
                </ConditionalPopover>
                <ConditionalPopover
                    conditions={{
                        isActive: !hasClusterNodeCertificate,
                        message: "You need to have a server certificate to replace it",
                    }}
                >
                    <Dropdown.Item
                        onClick={() => dispatch(certificatesActions.isReplaceServerModalOpenToggled())}
                        disabled={!hasClusterNodeCertificate}
                    >
                        <Icon icon="refresh" />
                        Replace server certificate
                    </Dropdown.Item>
                </ConditionalPopover>
            </Dropdown.Menu>
        </Dropdown>
    );
}
