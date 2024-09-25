import { Button, Col, Row, UncontrolledPopover } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { useAsync } from "react-async-hook";
import { CounterBadge } from "components/common/CounterBadge";
import FeatureNotAvailable from "components/common/FeatureNotAvailable";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { LicenseLimitReachStatus, getLicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import { useRavenLink } from "components/hooks/useRavenLink";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { DatabaseCustomSortersInfoHub } from "components/pages/database/settings/customSorters/DatabaseCustomSortersInfoHub";
import DatabaseCustomSortersList from "components/pages/database/settings/customSorters/DatabaseCustomSortersList";
import DatabaseCustomSortersServerWideList from "components/pages/database/settings/customSorters/DatabaseCustomSortersServerWideList";
import { useCustomSorters } from "components/common/customSorters/useCustomSorters";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import RichAlert from "components/common/RichAlert";

export default function DatabaseCustomSorters() {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const { databasesService, manageServerService } = useServices();

    const { sorters, setSorters, addNewSorter, removeSorter, mapFromDto } = useCustomSorters();

    // Changing the database causes re-mount
    const asyncGetDatabaseSorters = useAsync(() => databasesService.getCustomSorters(db.name), [], {
        onSuccess(result) {
            setSorters(mapFromDto(result));
        },
    });

    const asyncGetServerWideSorters = useAsync(async () => {
        if (!hasServerWideCustomSorters) {
            return [];
        }
        return await manageServerService.getServerWideCustomSorters();
    }, []);

    const { appUrl } = useAppUrls();
    const upgradeLicenseLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    const licenseClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomSortersPerCluster"));
    const licenseDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomSortersPerDatabase"));
    const numberOfCustomSortersInCluster = useAppSelector(licenseSelectors.limitsUsage).NumberOfCustomSortersInCluster;
    const hasServerWideCustomSorters = useAppSelector(licenseSelectors.statusValue("HasServerWideCustomSorters"));

    const databaseResultsCount = sorters.length;
    const serverWideResultsCount = asyncGetServerWideSorters.result?.length ?? null;

    const databaseLimitReachStatus = getLicenseLimitReachStatus(databaseResultsCount, licenseDatabaseLimit);
    const clusterLimitReachStatus = getLicenseLimitReachStatus(numberOfCustomSortersInCluster, licenseClusterLimit);

    const isLimitReached = databaseLimitReachStatus === "limitReached" || clusterLimitReachStatus === "limitReached";

    if (db.isSharded) {
        return (
            <FeatureNotAvailable>
                <span>
                    Custom sorters are not available for <Icon icon="sharding" color="shard" margin="m-0" /> sharded
                    databases
                </span>
            </FeatureNotAvailable>
        );
    }

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
                numberOfCustomSortersInCluster={numberOfCustomSortersInCluster}
                licenseClusterLimit={licenseClusterLimit}
                upgradeLicenseLink={upgradeLicenseLink}
            />
            <Col sm={12} lg={8}>
                <AboutViewHeading title="Custom sorters" icon="custom-sorters" />
                {hasDatabaseAdminAccess && (
                    <>
                        <div id="newCustomSorter" className="w-fit-content">
                            <Button color="primary" className="mb-3" onClick={addNewSorter} disabled={isLimitReached}>
                                <Icon icon="plus" />
                                Add a custom sorter
                            </Button>
                        </div>
                        {isLimitReached && (
                            <AddButtonLicensePopover
                                databaseLimitReachStatus={databaseLimitReachStatus}
                                upgradeLicenseLink={upgradeLicenseLink}
                            />
                        )}
                    </>
                )}

                <HrHeader count={databaseLimitReachStatus === "notReached" ? databaseResultsCount : null}>
                    Database custom sorters
                    {databaseLimitReachStatus !== "notReached" && (
                        <CounterBadge className="ms-2" count={databaseResultsCount} limit={licenseDatabaseLimit} />
                    )}
                </HrHeader>
                <DatabaseCustomSortersList
                    sorters={sorters}
                    fetchStatus={asyncGetDatabaseSorters.status}
                    reload={asyncGetDatabaseSorters.execute}
                    serverWideSorterNames={asyncGetServerWideSorters.result?.map((x) => x.Name) ?? []}
                    remove={removeSorter}
                />

                <HrHeader
                    right={
                        hasServerWideCustomSorters ? (
                            <a href={appUrl.forServerWideCustomSorters()} target="_blank">
                                <Icon icon="link" />
                                Server-wide custom sorters
                            </a>
                        ) : null
                    }
                    count={serverWideResultsCount}
                >
                    Server-wide custom sorters
                    {!hasServerWideCustomSorters && <LicenseRestrictedBadge licenseRequired="Professional +" />}
                </HrHeader>
                {hasServerWideCustomSorters && (
                    <DatabaseCustomSortersServerWideList asyncGetSorters={asyncGetServerWideSorters} />
                )}
            </Col>
            <Col sm={12} lg={4}>
                <DatabaseCustomSortersInfoHub databaseSortersCount={databaseResultsCount} />
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
        <RichAlert
            variant={databaseLimitReachStatus === "limitReached" ? "danger" : "warning"}
            icon="database"
            iconAddon="warning"
            className="mb-3"
        >
            Database {databaseLimitReachStatus === "limitReached" ? "has reached" : "is reaching"} the{" "}
            <strong>maximum number of Custom Sorters</strong> allowed per database by your license{" "}
            <strong>
                ({databaseResultsCount}/{licenseDatabaseLimit})
            </strong>
            <br /> Delete unused sorters or{" "}
            <strong>
                <a href={upgradeLicenseLink} target="_blank">
                    upgrade your license
                </a>
            </strong>
        </RichAlert>
    );
}

interface ClusterLimitAlertProps {
    clusterLimitReachStatus: LicenseLimitReachStatus;
    numberOfCustomSortersInCluster: number;
    licenseClusterLimit: number;
    upgradeLicenseLink: string;
}

function ClusterLimitAlert(props: ClusterLimitAlertProps) {
    const { clusterLimitReachStatus, numberOfCustomSortersInCluster, licenseClusterLimit, upgradeLicenseLink } = props;

    if (clusterLimitReachStatus === "notReached") {
        return null;
    }

    return (
        <RichAlert
            variant={clusterLimitReachStatus === "limitReached" ? "danger" : "warning"}
            icon="cluster"
            iconAddon="warning"
            className="mb-3"
        >
            Cluster {clusterLimitReachStatus === "limitReached" ? "has reached" : "is reaching"} the{" "}
            <strong>maximum number of Custom Sorters</strong> allowed per cluster by your license{" "}
            <strong>
                ({numberOfCustomSortersInCluster}/{licenseClusterLimit})
            </strong>
            <br /> Delete unused sorters or{" "}
            <strong>
                <a href={upgradeLicenseLink} target="_blank">
                    upgrade your license
                </a>
            </strong>
        </RichAlert>
    );
}

interface AddButtonLicensePopoverProps {
    databaseLimitReachStatus: LicenseLimitReachStatus;
    upgradeLicenseLink: string;
}

function AddButtonLicensePopover({ databaseLimitReachStatus, upgradeLicenseLink }: AddButtonLicensePopoverProps) {
    return (
        <UncontrolledPopover trigger="hover" target="newCustomSorter" placement="top" className="bs5">
            <div className="p-3 text-center">
                <Icon icon={databaseLimitReachStatus === "limitReached" ? "database" : "cluster"} />
                {databaseLimitReachStatus === "limitReached" ? "Database" : "Cluster"} has reached the maximum number of
                Custom Sorters allowed per {databaseLimitReachStatus === "limitReached" ? "database" : "cluster"}.
                <br /> Delete unused sorters or{" "}
                <a href={upgradeLicenseLink} target="_blank">
                    upgrade your license
                </a>
            </div>
        </UncontrolledPopover>
    );
}
