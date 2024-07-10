import React from "react";
import { Alert, Button, Col, Row, UncontrolledPopover } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { useAsync } from "react-async-hook";
import { CounterBadge } from "components/common/CounterBadge";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { getLicenseLimitReachStatus, LicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import { useRavenLink } from "components/hooks/useRavenLink";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { DatabaseCustomAnalyzersInfoHub } from "components/pages/database/settings/customAnalyzers/DatabaseCustomAnalyzersInfoHub";
import DatabaseCustomAnalyzersList from "components/pages/database/settings/customAnalyzers/DatabaseCustomAnalyzersList";
import { useCustomAnalyzers } from "components/common/customAnalyzers/useCustomAnalyzers";
import DatabaseCustomAnalyzersServerWideList from "components/pages/database/settings/customAnalyzers/DatabaseCustomAnalyzersServerWideList";

export default function DatabaseCustomAnalyzers() {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.hasDatabaseAdminAccess());

    const { databasesService, manageServerService } = useServices();

    const { analyzers, setAnalyzers, addNewAnalyzer, removeAnalyzer, mapFromDto } = useCustomAnalyzers();

    // Changing the database causes re-mount
    const asyncGetDatabaseAnalyzers = useAsync(() => databasesService.getCustomAnalyzers(db.name), [], {
        onSuccess(result) {
            setAnalyzers(mapFromDto(result));
        },
    });

    const asyncGetServerWideAnalyzers = useAsync(async () => {
        if (!hasServerWideCustomAnalyzers) {
            return [];
        }
        return await manageServerService.getServerWideCustomAnalyzers();
    }, []);

    const { appUrl } = useAppUrls();
    const upgradeLicenseLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    const licenseClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomAnalyzersPerCluster"));
    const licenseDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomAnalyzersPerDatabase"));
    const numberOfCustomAnalyzersInCluster = useAppSelector(licenseSelectors.limitsUsage).NumberOfAnalyzersInCluster;
    const hasServerWideCustomAnalyzers = useAppSelector(licenseSelectors.statusValue("HasServerWideAnalyzers"));

    const databaseResultsCount = analyzers.length;
    const serverWideResultsCount = asyncGetServerWideAnalyzers.result?.length ?? null;

    const databaseLimitReachStatus = getLicenseLimitReachStatus(databaseResultsCount, licenseDatabaseLimit);
    const clusterLimitReachStatus = getLicenseLimitReachStatus(numberOfCustomAnalyzersInCluster, licenseClusterLimit);

    const isLimitReached = databaseLimitReachStatus === "limitReached" || clusterLimitReachStatus === "limitReached";

    return (
        <Row className="gy-sm content-margin">
            <DatabaseLimitAlert
                databaseLimitReachStatus={databaseLimitReachStatus}
                databaseResultsCount={databaseResultsCount}
                licenseDatabaseLimit={licenseDatabaseLimit}
                upgradeLicenseLink={upgradeLicenseLink}
            />
            <ClusterLimitAlert
                clusterLimitReachStatus={clusterLimitReachStatus}
                numberOfCustomAnalyzersInCluster={numberOfCustomAnalyzersInCluster}
                licenseClusterLimit={licenseClusterLimit}
                upgradeLicenseLink={upgradeLicenseLink}
            />
            <Col>
                <AboutViewHeading title="Custom analyzers" icon="custom-analyzers" />
                {hasDatabaseAdminAccess && (
                    <>
                        <div id="newCustomAnalyzer" className="w-fit-content">
                            <Button color="primary" className="mb-3" onClick={addNewAnalyzer} disabled={isLimitReached}>
                                <Icon icon="plus" /> Add a custom analyzer
                            </Button>
                            {isLimitReached && (
                                <AddButtonLicensePopover
                                    databaseLimitReachStatus={databaseLimitReachStatus}
                                    upgradeLicenseLink={upgradeLicenseLink}
                                />
                            )}
                        </div>
                    </>
                )}

                <HrHeader count={databaseLimitReachStatus === "notReached" ? databaseResultsCount : null}>
                    Database custom analyzers
                    {databaseLimitReachStatus !== "notReached" && (
                        <CounterBadge className="ms-2" count={databaseResultsCount} limit={licenseDatabaseLimit} />
                    )}
                </HrHeader>
                <DatabaseCustomAnalyzersList
                    analyzers={analyzers}
                    fetchStatus={asyncGetDatabaseAnalyzers.status}
                    reload={asyncGetDatabaseAnalyzers.execute}
                    serverWideAnalyzerNames={asyncGetServerWideAnalyzers.result?.map((x) => x.Name) ?? []}
                    remove={removeAnalyzer}
                />

                <HrHeader
                    right={
                        <a
                            href={appUrl.forServerWideCustomAnalyzers()}
                            target="_blank"
                            title="Navigate to the server-wide view to edit"
                        >
                            <Icon icon="link" />
                            Server-wide custom analyzers
                        </a>
                    }
                    count={serverWideResultsCount}
                >
                    Server-wide custom analyzers
                    {!hasServerWideCustomAnalyzers && <LicenseRestrictedBadge licenseRequired="Professional +" />}
                </HrHeader>
                {hasServerWideCustomAnalyzers && (
                    <DatabaseCustomAnalyzersServerWideList asyncGetAnalyzers={asyncGetServerWideAnalyzers} />
                )}
            </Col>
            <Col sm={12} lg={4}>
                <DatabaseCustomAnalyzersInfoHub databaseAnalyzersCount={databaseResultsCount} />
            </Col>
        </Row>
    );
}

interface DatabaseLimitAlertProps {
    databaseLimitReachStatus: LicenseLimitReachStatus;
    databaseResultsCount: number;
    licenseDatabaseLimit: number;
    upgradeLicenseLink: string;
}

function DatabaseLimitAlert(props: DatabaseLimitAlertProps) {
    const { databaseLimitReachStatus, databaseResultsCount, licenseDatabaseLimit, upgradeLicenseLink } = props;

    if (databaseLimitReachStatus === "notReached") {
        return null;
    }

    return (
        <Alert color={databaseLimitReachStatus === "limitReached" ? "danger" : "warning"} className="text-center mb-3">
            <Icon icon="database" />
            Database {databaseLimitReachStatus === "limitReached" ? "has reached" : "is reaching"} the{" "}
            <strong>maximum number of Custom Analyzers</strong> allowed per database by your license{" "}
            <strong>
                ({databaseResultsCount}/{licenseDatabaseLimit})
            </strong>
            <br /> Delete unused analyzers or{" "}
            <strong>
                <a href={upgradeLicenseLink} target="_blank">
                    upgrade your license
                </a>
            </strong>
        </Alert>
    );
}

interface ClusterLimitAlertProps {
    clusterLimitReachStatus: LicenseLimitReachStatus;
    numberOfCustomAnalyzersInCluster: number;
    licenseClusterLimit: number;
    upgradeLicenseLink: string;
}

function ClusterLimitAlert(props: ClusterLimitAlertProps) {
    const { clusterLimitReachStatus, numberOfCustomAnalyzersInCluster, licenseClusterLimit, upgradeLicenseLink } =
        props;

    if (clusterLimitReachStatus === "notReached") {
        return null;
    }

    return (
        <Alert color={clusterLimitReachStatus === "limitReached" ? "danger" : "warning"} className="text-center mb-3">
            <Icon icon="cluster" />
            Cluster {clusterLimitReachStatus === "limitReached" ? "has reached" : "is reaching"} the{" "}
            <strong>maximum number of Custom Analyzers</strong> allowed per cluster by your license{" "}
            <strong>
                ({numberOfCustomAnalyzersInCluster}/{licenseClusterLimit})
            </strong>
            <br /> Delete unused analyzers or{" "}
            <strong>
                <a href={upgradeLicenseLink} target="_blank">
                    upgrade your license
                </a>
            </strong>
        </Alert>
    );
}

interface AddButtonLicensePopoverProps {
    databaseLimitReachStatus: LicenseLimitReachStatus;
    upgradeLicenseLink: string;
}

function AddButtonLicensePopover({ databaseLimitReachStatus, upgradeLicenseLink }: AddButtonLicensePopoverProps) {
    return (
        <UncontrolledPopover trigger="hover" target="newCustomAnalyzer" placement="top" className="bs5">
            <div className="p-3 text-center">
                <Icon icon={databaseLimitReachStatus === "limitReached" ? "database" : "cluster"} />
                {databaseLimitReachStatus === "limitReached" ? "Database" : "Cluster"} has reached the maximum number of
                Custom Analyzers allowed per {databaseLimitReachStatus === "limitReached" ? "database" : "cluster"}.
                <br /> Delete unused analyzers or{" "}
                <a href={upgradeLicenseLink} target="_blank">
                    upgrade your license
                </a>
            </div>
        </UncontrolledPopover>
    );
}
