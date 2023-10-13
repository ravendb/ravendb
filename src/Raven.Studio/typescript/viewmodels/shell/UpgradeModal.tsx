import { todo } from "common/developmentHelper";
import { Icon } from "components/common/Icon";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import React from "react";
import { Modal, ModalBody, ModalFooter } from "reactstrap";

const upgradeLicenseImg = require("Content/img/upgrade-license.svg");

todo("Styling", "ANY", "scale for larger screens?");

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
            isOpen
            toggle={null}
            wrapClassName="bs5"
            centered
            contentClassName="modal-border bulge-warning"
            zIndex={1080}
        >
            <ModalBody className="vstack gap-3 position-relative justify-content-center">
                <div className="d-flex justify-content-center">
                    <img src={upgradeLicenseImg} alt="Upgrade license" width="120" />
                </div>
                <h3 className="text-warning text-center">It&apos;s time to upgrade!</h3>
                <p className="text-center">
                    Your server is running version <strong>{productVersion}</strong> while the latest version is{" "}
                    <strong>{latestVersion}</strong>.
                    <br />
                    In order to continue using RavenDB please upgrade your server to the latest available version.
                </p>
            </ModalBody>
            <ModalFooter className="justify-content-center">
                <a href={downloadLink} target="_blank" className="btn btn-warning rounded-pill">
                    <Icon icon="download" />
                    Download now
                </a>
            </ModalFooter>
        </Modal>
    );
}
