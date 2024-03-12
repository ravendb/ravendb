import { Button, Modal, ModalBody, ModalFooter, UncontrolledPopover, UncontrolledTooltip } from "reactstrap";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import React, { ReactNode, useState } from "react";
import { aboutPageUrls } from "components/pages/resources/about/partials/common";
import { useServices } from "hooks/useServices";
import { useAsync } from "react-async-hook";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import genUtils from "common/generalUtils";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import CustomPagination from "components/common/Pagination";

interface ChangelogModalProps {
    mode: "whatsNew" | "changeLog" | "hidden";
    onClose: () => void;
}

export function ChangeLogModal(props: ChangelogModalProps) {
    const { mode, onClose } = props;
    const { licenseService } = useServices();
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

    const [page, setPage] = useState(1);

    const asyncGetChangeLog = useAsync(
        () => licenseService.getChangeLog((page - 1) * changeLogItemsPerPage, changeLogItemsPerPage),
        [page]
    );

    if (asyncGetChangeLog.loading) {
        return (
            <ModalWrapper onClose={onClose} mode={mode}>
                <LazyLoad active>
                    <h3>VERSION</h3>
                </LazyLoad>

                <LazyLoad active>
                    <div style={{ height: 200 }}></div>
                </LazyLoad>
            </ModalWrapper>
        );
    }

    if (asyncGetChangeLog.status === "error") {
        return (
            <ModalWrapper onClose={onClose} mode={mode}>
                <div className="m-3">
                    <LoadError />
                </div>
            </ModalWrapper>
        );
    }

    if (!asyncGetChangeLog.result) {
        console.error("Change log isn't available in the Developer build.");
        return null;
    }

    const canUpgrade = asyncGetChangeLog.result.IsLicenseEligibleForUpgrade;
    const versionsList =
        mode === "whatsNew"
            ? asyncGetChangeLog.result.BuildCompatibilitiesForLatestMajorMinor
            : asyncGetChangeLog.result.BuildCompatibilitiesForUserMajorMinor;

    return (
        <ModalWrapper onClose={onClose} mode={mode}>
            <div className="changelog-modal">
                {!isCloud && mode === "whatsNew" && (
                    <>
                        <div
                            className="well px-3 mx-auto w-fit-content mb-4 py-1 small rounded-pill"
                            id="updateLicenseInfo"
                        >
                            {canUpgrade ? (
                                <>
                                    <Icon icon="check" color="success" /> Your license is compatible{" "}
                                </>
                            ) : (
                                <>
                                    <Icon icon="license" color="warning" /> Your license needs to be upgraded in order
                                    to update{" "}
                                </>
                            )}
                        </div>
                        <UncontrolledPopover trigger="hover" className="bs5" placement="top" target="updateLicenseInfo">
                            <div className="px-2 py-1">
                                {canUpgrade ? (
                                    <>Your license can be used with the target version</>
                                ) : (
                                    <>
                                        Your license can&apos;t be used with the target version. Prior updating, please
                                        contact Sales and update your license beforehand.
                                    </>
                                )}
                            </div>
                        </UncontrolledPopover>
                    </>
                )}

                {versionsList.map((build, index) => {
                    const downgradeTooltipId = `canDowngradeTooltip-${index}`;

                    return (
                        <div key={build.FullVersion} className="mb-5">
                            <div className="d-flex align-items-center justify-content-between">
                                <h3>
                                    {mode === "whatsNew" && (
                                        <>
                                            <strong className="text-warning">
                                                <Icon icon="star-filled" />
                                                NEW
                                            </strong>{" "}
                                            -{" "}
                                        </>
                                    )}
                                    {build.FullVersion} -{" "}
                                    {genUtils.formatUtcDateAsLocal(build.ReleasedAt, genUtils.basicDateFormat)}{" "}
                                </h3>
                                {!isCloud && build.CanDowngradeFollowingUpgrade && (
                                    <>
                                        <div className="well px-3 py-1 small rounded-pill" id={downgradeTooltipId}>
                                            <Icon icon="check" color="success" /> Can downgrade
                                        </div>
                                        <UncontrolledTooltip
                                            trigger="hover"
                                            className="bs5"
                                            placement="top"
                                            target={downgradeTooltipId}
                                        >
                                            <div className="px-2 py-1">
                                                This update allows you to switch back to the current version
                                            </div>
                                        </UncontrolledTooltip>
                                    </>
                                )}
                            </div>
                            <div
                                className="well rounded-3 p-4 mt-2 vstack changelog-content"
                                dangerouslySetInnerHTML={{ __html: build.ChangelogHtml }}
                            ></div>
                        </div>
                    );
                })}

                <div className="mt-1">
                    <CustomPagination
                        page={page}
                        totalPages={versionsList.length / changeLogItemsPerPage}
                        onPageChange={setPage}
                    />
                </div>
            </div>
        </ModalWrapper>
    );
}

function ModalWrapper(props: { children: ReactNode } & ChangelogModalProps) {
    const { onClose, children, mode } = props;
    return (
        <Modal
            isOpen
            toggle={onClose}
            wrapClassName="bs5"
            centered
            size="lg"
            contentClassName="modal-border bulge-warning"
        >
            <ModalBody className="vstack gap-4 position-relative">
                <div className="text-center">
                    <Icon icon="logs" color="warning" className="fs-1" margin="m-0" />
                </div>

                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={onClose} />
                </div>
                <div className="text-center lead">{mode === "whatsNew" ? "What's New" : "Changelog"}</div>
                {children}
            </ModalBody>
            <ModalFooter>
                <Button color="secondary" outline onClick={onClose} className="rounded-pill px-3">
                    Close
                </Button>

                {mode === "whatsNew" && (
                    <React.Fragment key="footer-part">
                        <FlexGrow />
                        <Button color="primary" className="rounded-pill px-3" href={aboutPageUrls.updateInstructions}>
                            Update instructions <Icon icon="newtab" margin="m-0" />
                        </Button>
                    </React.Fragment>
                )}
            </ModalFooter>
        </Modal>
    );
}

const changeLogItemsPerPage = 10;
