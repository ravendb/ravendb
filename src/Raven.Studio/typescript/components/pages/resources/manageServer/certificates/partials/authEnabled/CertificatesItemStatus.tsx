import { RichPanelStatus } from "components/common/RichPanel";
import { CertificatesState } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";

interface CertificatesItemStatusProps {
    state: CertificatesState;
}

export default function CertificatesItemStatus({ state }: CertificatesItemStatusProps) {
    const statusText = state === "About to expire" ? "Valid" : state;

    return <RichPanelStatus color={certificatesUtils.getStateColor(state)}>{statusText}</RichPanelStatus>;
}
