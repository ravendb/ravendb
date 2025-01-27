import { AboutViewHeading } from "components/common/AboutView";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import CertificatesAuthDisabled from "components/pages/resources/manageServer/certificates/partials/authDisabled/CertificatesAuthDisabled";
import CertificatesAuthEnabled from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesAuthEnabled";
import { CertificatesInfoHub } from "components/pages/resources/manageServer/certificates/partials/CertificatesInfoHub";
import { useAppSelector } from "components/store";

export default function Certificates() {
    const isSecureServer = useAppSelector(accessManagerSelectors.isSecureServer);

    return (
        <div className="content-padding">
            <div className="hstack justify-content-between">
                <AboutViewHeading title="Certificates" icon="certificate" />
                <CertificatesInfoHub />
            </div>
            {isSecureServer ? <CertificatesAuthEnabled /> : <CertificatesAuthDisabled />}
        </div>
    );
}
