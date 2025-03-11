import { Icon } from "components/common/Icon";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import React from "react";
import Modal from "components/common/Modal";

const upgradeLicenseImg = require("Content/img/upgrade-license.svg");

export default function UpgradeModal() {
    const downloadLink = useRavenLink({ hash: "44DYH5", isDocs: false });

    const upgradeRequired = useAppSelector(licenseSelectors.statusValue("UpgradeRequired"));
    const latestVersion = useAppSelector(licenseSelectors.statusValue("Version"));
    const productVersion = useAppSelector(clusterSelectors.serverVersion)?.ProductVersion;

    if (!upgradeRequired || !latestVersion || !productVersion) {
        return null;
    }

    return (
        <Modal
            show
            contentClassName="modal-border bulge-warning"
            size="lg"
        >
            <Modal.Body className="vstack gap-3 position-relative justify-content-center">
                <div className="d-flex justify-content-center mb-3">
                    <img src={upgradeLicenseImg} alt="Upgrade license" width="120" />
                </div>
                <h3 className="text-warning text-center mb-0">It&apos;s time to upgrade!</h3>
                <p className="text-center mb-0">
                    Your server is running version <strong>{productVersion}</strong> while the latest version is{" "}
                    <strong>{latestVersion}</strong>.
                    <br />
                    In order to continue using RavenDB please upgrade your server to the latest available version.
                </p>
            </Modal.Body>
            <Modal.Footer className="justify-content-center">
                <a href={downloadLink} target="_blank" className="btn btn-warning rounded-pill">
                    <Icon icon="download" />
                    Download now
                </a>
            </Modal.Footer>
        </Modal>
    );
}
