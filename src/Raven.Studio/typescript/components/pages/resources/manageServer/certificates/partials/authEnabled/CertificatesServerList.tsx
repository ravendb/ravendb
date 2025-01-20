import { HrHeader } from "components/common/HrHeader";
import { Icon } from "components/common/Icon";
import CertificatesListItem from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesListItem";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { useAppSelector } from "components/store";

export default function CertificatesServerList() {
    const serverCertificateThumbprint = useAppSelector(certificatesSelectors.serverCertificateThumbprint);
    const filteredCertificates = useAppSelector(certificatesSelectors.filteredCertificates).filter((cert) =>
        cert.Thumbprints.includes(serverCertificateThumbprint)
    );

    return (
        <div>
            <HrHeader>
                <Icon icon="server" />
                Server
            </HrHeader>
            {filteredCertificates.map((cert) => (
                <CertificatesListItem key={cert.Thumbprint} certificate={cert} />
            ))}
        </div>
    );
}
