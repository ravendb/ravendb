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
import Button from "react-bootstrap/Button";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import Modal from "components/common/Modal";

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

    const versionsList =
        mode === "whatsNew"
            ? asyncGetChangeLog.result.BuildCompatibilitiesForLatestMajorMinor
            : asyncGetChangeLog.result.BuildCompatibilitiesForUserMajorMinor;

    const totalResults =
        mode === "whatsNew"
            ? asyncGetChangeLog.result.TotalBuildsForLatestMajorMinor
            : asyncGetChangeLog.result.TotalBuildsForUserMajorMinor;

    return (
        <ModalWrapper
            onClose={onClose}
            mode={mode}
            page={page}
            totalPages={Math.ceil(totalResults / changeLogItemsPerPage)}
            onPageChange={setPage}
        >
            <div className="changelog-modal">
                {versionsList.map((build, index) => {
                    const downgradeTooltipId = `canDowngradeTooltip-${index}`;
                    const upgradeTooltipId = `canUpgradeTooltip-${index}`;

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
                                <div className="flex-horizontal">
                                    {!isCloud && (
                                        <React.Fragment key="upgrade-downgrade-info">
                                            <OverlayTrigger
                                                overlay={
                                                    <Tooltip id={downgradeTooltipId}>
                                                        <div className="px-2 py-1">
                                                            {build.CanDowngradeFollowingUpgrade ? (
                                                                <>
                                                                    This update allows you to switch back to the current
                                                                    version
                                                                </>
                                                            ) : (
                                                                <>
                                                                    This update doesn&apos;t allow you to switch back to
                                                                    the current version
                                                                </>
                                                            )}
                                                        </div>
                                                    </Tooltip>
                                                }
                                            >
                                                <div className="well mx-1 px-3 py-1 small rounded-pill">
                                                    {build.CanDowngradeFollowingUpgrade ? (
                                                        <>
                                                            <Icon icon="check" color="success" /> Can downgrade
                                                        </>
                                                    ) : (
                                                        <>
                                                            <Icon icon="cancel" color="danger" /> Can&apos;t downgrade
                                                        </>
                                                    )}
                                                </div>
                                            </OverlayTrigger>
                                            <OverlayTrigger
                                                overlay={
                                                    <Tooltip id={upgradeTooltipId}>
                                                        <div className="px-2 py-1">
                                                            {build.CanUpgrade ? (
                                                                <>
                                                                    Your license is eligible for upgrade to this version
                                                                </>
                                                            ) : (
                                                                <>
                                                                    Your license can&apos;t be used with the target
                                                                    version. Prior updating, please contact us and
                                                                    update your license beforehand.
                                                                </>
                                                            )}
                                                        </div>
                                                    </Tooltip>
                                                }
                                            >
                                                <div className="well mx-1 px-3 py-1 small rounded-pill">
                                                    {build.CanUpgrade ? (
                                                        <>
                                                            <Icon icon="check" color="success" /> Can upgrade
                                                        </>
                                                    ) : (
                                                        <>
                                                            <Icon icon="license" color="danger" /> Your license needs to
                                                            be upgraded in order to update
                                                        </>
                                                    )}
                                                </div>
                                            </OverlayTrigger>
                                        </React.Fragment>
                                    )}
                                </div>
                            </div>
                            <div
                                className="well rounded-3 p-4 mt-2 vstack changelog-content"
                                dangerouslySetInnerHTML={{ __html: build.ChangelogHtml }}
                            ></div>
                        </div>
                    );
                })}
            </div>
        </ModalWrapper>
    );
}

function ModalWrapper(
    props: {
        children: ReactNode;
        page?: number;
        totalPages?: number;
        onPageChange?: (page: number) => void;
    } & ChangelogModalProps
) {
    const { onClose, children, mode, page, totalPages, onPageChange } = props;
    return (
        <Modal show scrollable onHide={onClose} size="lg" contentClassName="modal-border bulge-warning">
            <Modal.Header className="vstack gap-4" onCloseClick={onClose}>
                <Icon icon="logs" color="warning" className="fs-1" margin="m-0" />
                <div className=" lead">{mode === "whatsNew" ? "What's New" : "Changelog"}</div>
            </Modal.Header>
            <Modal.Body className="vstack gap-4 position-relative">{children}</Modal.Body>
            <Modal.Footer>
                <Button variant="outline-secondary" onClick={onClose} className="rounded-pill px-3">
                    Close
                </Button>

                {mode === "whatsNew" && (
                    <React.Fragment key="footer-part">
                        <CustomPagination page={page} totalPages={totalPages} onPageChange={onPageChange} />
                        <FlexGrow />
                        <Button
                            variant="outline-primary"
                            className="rounded-pill px-3"
                            href={aboutPageUrls.updateInstructions}
                        >
                            Update instructions <Icon icon="newtab" margin="m-0" />
                        </Button>
                    </React.Fragment>
                )}
            </Modal.Footer>
        </Modal>
    );
}

const changeLogItemsPerPage = 2;
