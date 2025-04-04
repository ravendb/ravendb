import { AboutViewHeading } from "components/common/AboutView";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import CertificatesAuthDisabled from "components/pages/resources/manageServer/certificates/partials/authDisabled/CertificatesAuthDisabled";
import CertificatesAuthEnabled from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesAuthEnabled";
import { useAppSelector } from "components/store";
import { CertificatesInfoHub } from "viewmodels/manage/CertificatesInfoHub";

export default function Certificates() {
    const isSecureServer = useAppSelector(accessManagerSelectors.isSecureServer);

    return (
        <div className="content-padding">
            <AboutViewHeading title="Certificates" icon="certificate" />
            <CertificatesInfoHub />

            {isSecureServer ? <CertificatesAuthEnabled /> : <CertificatesAuthDisabled />}
        </div>
    );
}
