import { HrHeader } from "components/common/HrHeader";
import { Icon } from "components/common/Icon";
import CertificatesListItem from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesListItem";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { useAppSelector } from "components/store";
import Badge from "react-bootstrap/Badge";

export default function CertificatesSsoList() {
    const filteredCertificates = useAppSelector(certificatesSelectors.filteredCertificates);

    const ssoServerCerts = filteredCertificates.filter((c) => c.Usage === "SsoServer");
    const ssoUserCerts = filteredCertificates.filter((c) => c.Usage === "SsoClient");

    const allSsoServerCerts = useAppSelector(certificatesSelectors.ssoServerCertificates);
    const allSsoUserCerts = useAppSelector(certificatesSelectors.ssoUserCertificates);

    if (!ssoServerCerts.length && !ssoUserCerts.length) {
        return null;
    }

    return (
        <div>
            <HrHeader count={allSsoServerCerts.length + allSsoUserCerts.length}>
                <Icon icon="key" />
                SSO
            </HrHeader>
            {ssoServerCerts.length > 0 && (
                <>
                    <div className="ms-1 mb-2">
                        <Badge bg="secondary" className="me-2">
                            <Icon icon="certificate" margin="me-1" />
                            {` Certificates: ${allSsoServerCerts.length}`}
                        </Badge>
                    </div>
                    {ssoServerCerts.map((cert) => (
                        <CertificatesListItem key={cert.Thumbprint} certificate={cert} />
                    ))}
                </>
            )}
            {ssoUserCerts.length > 0 && (
                <>
                    <div className="ms-1 mb-2 mt-3">
                        <Badge bg="secondary" className="me-2">
                            <Icon icon="user" margin="me-1" />
                            {` Users: ${allSsoUserCerts.length}`}
                        </Badge>
                    </div>
                    {ssoUserCerts.map((cert) => (
                        <CertificatesListItem key={cert.Thumbprint} certificate={cert} />
                    ))}
                </>
            )}
        </div>
    );
}
