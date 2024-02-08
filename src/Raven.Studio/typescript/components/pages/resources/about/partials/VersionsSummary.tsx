import { Button, Card, CardBody, Col, Row } from "reactstrap";
import { aboutPageUrls, OverallInfoItem } from "components/pages/resources/about/partials/common";
import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { AsyncState, UseAsyncReturn } from "react-async-hook";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

interface VersionsSummaryProps {
    asyncLatestVersion: AsyncState<Raven.Server.ServerWide.BackgroundTasks.LatestVersionCheck.VersionInfo>;
    refreshLatestVersion: () => Promise<void>;
    toggleShowChangelogModal: () => void;
}

export function VersionsSummary(props: VersionsSummaryProps) {
    const clientVersion = useAppSelector(clusterSelectors.clientVersion);
    const serverVersion = useAppSelector(clusterSelectors.serverVersion);
    const serverFullVersion = serverVersion?.FullVersion ?? "n/a";

    const { asyncLatestVersion } = props;

    const [refreshing, setRefreshing] = useState<boolean>(false);

    const { toggleShowChangelogModal, refreshLatestVersion } = props;

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
                    <OverallInfoItem icon="client" label="Studio version">
                        {clientVersion}
                    </OverallInfoItem>
                    <OverallInfoItem icon="global" label="Updates">
                        <LatestVersion asyncLatestVersion={asyncLatestVersion} serverVersion={serverVersion} />
                    </OverallInfoItem>
                    <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
                        <Button outline className="rounded-pill" onClick={toggleShowChangelogModal}>
                            <Icon icon="logs" /> Changelog
                        </Button>
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
}) {
    const { asyncLatestVersion, serverVersion } = props;

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
                <div className="small text-muted fw-light">{latestVersion}</div>
                <a href={aboutPageUrls.updateInstructions} className="small" target="_blank">
                    Update instructions <Icon icon="newtab" margin="m-0" />
                </a>
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
