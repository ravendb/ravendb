import { EmptySet } from "components/common/EmptySet";
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

    if (allSsoServerCerts.length === 0 && allSsoUserCerts.length === 0) {
        return null;
    }

    return (
        <div>
            <HrHeader count={allSsoServerCerts.length + allSsoUserCerts.length}>
                <Icon icon="lock" />
                SSO
            </HrHeader>
            <div className="ms-1 mb-2">
                <Badge bg="faded-primary" className="me-2">
                    <Icon icon="certificate" margin="m-0" />
                    {` Certificates: ${allSsoServerCerts.length}`}
                </Badge>
            </div>
            {!ssoServerCerts.length && <EmptySet compact>No SSO certificates</EmptySet>}
            {ssoServerCerts.map((cert) => (
                <CertificatesListItem key={cert.Thumbprint} certificate={cert} />
            ))}
            <div className="ms-1 mb-2 mt-3">
                <Badge bg="faded-primary" className="me-2">
                    <Icon icon="user" margin="m-0" />
                    {` Users: ${allSsoUserCerts.length}`}
                </Badge>
            </div>
            {!ssoUserCerts.length && <EmptySet compact>No SSO users</EmptySet>}
            {ssoUserCerts.map((cert) => (
                <CertificatesListItem key={cert.Thumbprint} certificate={cert} />
            ))}
        </div>
    );
}
