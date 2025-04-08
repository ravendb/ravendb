import { Icon } from "components/common/Icon";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import Modal from "components/common/Modal";
import moment from "moment";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { LicenseTooltip } from "components/pages/resources/about/partials/LicenseSummary";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAsync } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import registration from "viewmodels/shell/registration";
import Button from "react-bootstrap/Button";

const upgradeLicenseImg = require("Content/img/upgrade-license.svg");

export default function UpgradeModal(props: { close: () => void }) {
    const downloadLink = useRavenLink({ hash: "44DYH5", isDocs: false });

    const upgradeRequired = useAppSelector(licenseSelectors.statusValue("UpgradeRequired"));
    const latestVersion = useAppSelector(licenseSelectors.statusValue("Version"));
    const productVersion = useAppSelector(clusterSelectors.serverVersion)?.ProductVersion;
    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);

    const { licenseService } = useServices();
    const asyncGetConfigurationSettings = useAsync(licenseService.getConfigurationSettings, []);
    const canActivate = asyncGetConfigurationSettings.result?.CanActivate;
    const isReplaceLicenseEnabled = canActivate && isClusterAdminOrClusterNode;

    const licenseStatus = useAppSelector(licenseSelectors.status);
    
    const registerLicense = () => {
        registration.showRegistrationDialog(licenseStatus, false, true);

        // Remove tabindex from bs5 modal for bs3 to work
        const upgradeModalElement = document.querySelector("#bs5-modal [role='dialog']");
        if (upgradeModalElement && upgradeModalElement.hasAttribute("tabindex")) {
            upgradeModalElement.removeAttribute("tabindex");
        }
    };

    if (!upgradeRequired || !latestVersion || !productVersion) {
        return null;
    }

    const allowDismissUntilUtc = moment.utc(upgradeRequired.AllowDismissUntil);

    return (
        <Modal show contentClassName="modal-border bulge-warning" size="lg">
            <Modal.Header
                className="vstack gap-4"
                closeButton={upgradeRequired.AllowDismiss}
                onCloseClick={props.close}
            >
                <div className="d-flex justify-content-center">
                    <img src={upgradeLicenseImg} alt="Upgrade license" width="120" />
                </div>
                <h3 className="text-warning text-center mb-0">It&apos;s time to upgrade!</h3>
            </Modal.Header>
            <Modal.Body>
                <p className="text-center mb-0">
                    Your server is running version <strong>{productVersion}</strong> while the latest version is{" "}
                    <strong>{latestVersion}</strong>.
                    <br />
                    In order to continue using RavenDB please upgrade your server to the latest available version.
                    {upgradeRequired.AllowDismiss && upgradeRequired.AllowDismissUntil && (
                        <>
                            <br />
                            <br />
                            <span className="text-muted">
                                You can dismiss this message until {allowDismissUntilUtc.format(dateFormat)} UTC
                                <PopoverWithHoverWrapper
                                    message={`${allowDismissUntilUtc.local().format(dateFormat)} your local time`}
                                >
                                    <Icon icon="info" color="info" margin="ms-1" />
                                </PopoverWithHoverWrapper>
                            </span>
                        </>
                    )}
                </p>
            </Modal.Body>
            <Modal.Footer className="hstack gap-2 justify-content-center">
                <LicenseTooltip
                    target="replace-license-btn"
                    operationEnabledInConfiguration={canActivate}
                    hasPrivileges={isClusterAdminOrClusterNode}
                    operationAction="Replace the current license with another"
                    operationTitle="Replacing license"
                >
                    <span>
                        <Button
                            variant="outline-secondary"
                            className="rounded-pill"
                            onClick={registerLicense}
                            disabled={!isReplaceLicenseEnabled}
                        >
                            <Icon icon="replace" /> Replace
                        </Button>
                    </span>
                </LicenseTooltip>
                <a href={downloadLink} target="_blank" className="btn btn-warning rounded-pill">
                    <Icon icon="download" />
                    Download now
                </a>
            </Modal.Footer>
        </Modal>
    );
}

const dateFormat = "YYYY-MM-DD HH:mm";
