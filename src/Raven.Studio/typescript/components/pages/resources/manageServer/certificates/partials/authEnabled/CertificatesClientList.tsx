import { EmptySet } from "components/common/EmptySet";
import { HrHeader } from "components/common/HrHeader";
import { Icon } from "components/common/Icon";
import CertificatesListItem from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesListItem";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { useAppSelector } from "components/store";

export default function CertificatesClientList() {
    const serverCertificateThumbprint = useAppSelector(certificatesSelectors.serverCertificateThumbprint);

    const allClientCertsCount = useAppSelector(certificatesSelectors.certificates).filter(
        (cert) => !cert.Thumbprints.includes(serverCertificateThumbprint)
    ).length;

    const filteredCertificates = useAppSelector(certificatesSelectors.filteredCertificates).filter(
        (cert) => !cert.Thumbprints.includes(serverCertificateThumbprint)
    );

    return (
        <div>
            <HrHeader count={allClientCertsCount}>
                <Icon icon="client" />
                Client
            </HrHeader>
            {!filteredCertificates.length && <EmptySet compact>No certificates</EmptySet>}
            {filteredCertificates.map((cert) => (
                <CertificatesListItem key={cert.Thumbprint} certificate={cert} />
            ))}
        </div>
    );
}
