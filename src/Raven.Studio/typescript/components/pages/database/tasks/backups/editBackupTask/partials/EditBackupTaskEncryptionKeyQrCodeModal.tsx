import { useEffect, useRef } from "react";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import { encryptionKeyQrCodeUtils } from "components/utils/encryptionKeyQrCodeUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface EditBackupTaskEncryptionKeyQrCodeModalProps {
    encryptionKey: string;
    onClose: () => void;
}

export default function EditBackupTaskEncryptionKeyQrCodeModal({
    encryptionKey,
    onClose,
}: EditBackupTaskEncryptionKeyQrCodeModalProps) {
    const qrContainerRef = useRef<HTMLDivElement>(null);
    const t = useStudioTranslation("editBackupTask");
    const commonT = useStudioTranslation("common");

    // qrcodejs draws into a DOM node, so the code is rendered after the container is mounted.
    useEffect(() => {
        const qrCode = encryptionKeyQrCodeUtils.renderQrCode(qrContainerRef.current, encryptionKey, 196);
        return () => qrCode.clear();
    }, [encryptionKey]);

    return (
        <Modal show onHide={onClose} contentClassName="modal-border bulge-primary">
            <Modal.Header closeButton onCloseClick={onClose} className="pb-0">
                <h3 className="hstack gap-3 mb-0">
                    <Icon icon="encryption" color="primary" margin="m-0" />
                    {t("encryption.qrCodeTitle")}
                </h3>
            </Modal.Header>
            <Modal.Body className="vstack gap-4 align-items-center">
                <p className="mb-0 w-100">{t("encryption.qrCodeInfo")}</p>
                <div className="edit-backup-task-qr-code">
                    <div ref={qrContainerRef} />
                    <img src={encryptionKeyQrCodeUtils.qrLogo} alt="" className="edit-backup-task-qr-code-logo" />
                </div>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" className="link-muted" onClick={onClose}>
                    {commonT("close")}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
