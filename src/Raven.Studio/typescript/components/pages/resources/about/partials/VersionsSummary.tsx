import { Button, Card, CardBody, Col, Row } from "reactstrap";
import { OverallInfoItem } from "components/pages/resources/about/partials/common";
import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { AsyncState } from "react-async-hook";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { licenseSelectors } from "components/common/shell/licenseSlice";

interface VersionsSummaryProps {
    asyncLatestVersion: AsyncState<Raven.Server.ServerWide.BackgroundTasks.LatestVersionCheck.VersionInfo>;
    refreshLatestVersion: () => Promise<void>;
    showChangeLogModal: () => void;
    showWhatsNewModal: () => void;
}

export function VersionsSummary(props: VersionsSummaryProps) {
    const serverVersion = useAppSelector(clusterSelectors.serverVersion);
    const serverFullVersion = serverVersion?.FullVersion ?? "n/a";
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const { asyncLatestVersion } = props;

    const showLatestVersionInfo = !isCloud && !!asyncLatestVersion.result;

    const [refreshing, setRefreshing] = useState<boolean>(false);

    const { showChangeLogModal, showWhatsNewModal, refreshLatestVersion } = props;

    const checkForUpdates = async () => {
        setRefreshing(true);
        try {
            await refreshLatestVersion();
        } finally {
            setRefreshing(false);
        }
    };

    return (
        <Card>
            <CardBody>
                <h4>Software Version</h4>
                <Row>
                    <OverallInfoItem icon="server" label="Server version">
                        {serverFullVersion}
                    </OverallInfoItem>
                    <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
                        <Button outline className="rounded-pill" onClick={showChangeLogModal}>
                            <Icon icon="logs" /> Changelog
                        </Button>
                    </Col>
                </Row>
                {showLatestVersionInfo && (
                    <Row>
                        <OverallInfoItem icon="global" label="Updates">
                            <LatestVersion
                                asyncLatestVersion={asyncLatestVersion}
                                serverVersion={serverVersion}
                                showWhatsNewModal={showWhatsNewModal}
                            />
                        </OverallInfoItem>
                        <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
                            <ButtonWithSpinner
                                isSpinning={refreshing}
                                outline
                                className="rounded-pill"
                                onClick={checkForUpdates}
                            >
                                <Icon icon="refresh" /> Check for updates
                            </ButtonWithSpinner>
                        </Col>
                    </Row>
                )}
            </CardBody>
        </Card>
    );
}

function isNewVersionAvailable(
    serverVersion: serverBuildVersionDto,
    latestVersion: Raven.Server.ServerWide.BackgroundTasks.LatestVersionCheck.VersionInfo
) {
    if (!latestVersion) {
        return false;
    }

    if (!serverVersion) {
        return false;
    }

    const isDevBuildNumber = (num: number) => num >= 40 && num < 90;
    return !isDevBuildNumber(latestVersion.BuildNumber) && latestVersion.BuildNumber > serverVersion.BuildVersion;
}

function LatestVersion(props: {
    asyncLatestVersion: AsyncState<Raven.Server.ServerWide.BackgroundTasks.LatestVersionCheck.VersionInfo>;
    serverVersion: serverBuildVersionDto;
    showWhatsNewModal: () => void;
}) {
    const { asyncLatestVersion, serverVersion, showWhatsNewModal } = props;

    if (asyncLatestVersion.loading) {
        return (
            <LazyLoad active>
                <div>Loading placeholder</div>
            </LazyLoad>
        );
    }

    if (asyncLatestVersion.status === "error") {
        return <LoadError error="Unable to load latest version" />;
    }

    const hasNewerVersion = isNewVersionAvailable(serverVersion, asyncLatestVersion.result);

    if (hasNewerVersion) {
        const latestVersion = asyncLatestVersion.result.Version;

        return (
            <>
                <span className="text-warning">
                    <Icon icon="star-filled" />
                    Update Available
                </span>
                <div className="small text-muted fw-light">
                    {latestVersion}
                    <Button size="xs" color="link" onClick={showWhatsNewModal} className="fw-bold">
                        What&apos;s new?
                    </Button>
                </div>
            </>
        );
    }

    return (
        <span className="text-success">
            <Icon icon="check" />
            You are using the latest version
        </span>
    );
}
