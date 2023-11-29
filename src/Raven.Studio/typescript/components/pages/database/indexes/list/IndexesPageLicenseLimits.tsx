import { Icon } from "components/common/Icon";
import { LicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import React from "react";
import { Alert } from "reactstrap";

interface IndexesPageLicenseLimitsProps {
    staticClusterLimitStatus: LicenseLimitReachStatus;
    staticClusterCount: number;
    staticClusterLimit: number;
    upgradeLicenseLink: string;
    autoClusterLimitStatus: LicenseLimitReachStatus;
    autoClusterCount: number;
    autoClusterLimit: number;
    staticDatabaseLimitStatus: LicenseLimitReachStatus;
    staticDatabaseCount: number;
    staticDatabaseLimit: number;
    autoDatabaseLimitStatus: LicenseLimitReachStatus;
    autoDatabaseCount: number;
    autoDatabaseLimit: number;
}

export default function IndexesPageLicenseLimits({
    staticClusterLimitStatus,
    staticClusterCount,
    staticClusterLimit,
    upgradeLicenseLink,
    autoClusterLimitStatus,
    autoClusterCount,
    autoClusterLimit,
    staticDatabaseLimitStatus,
    staticDatabaseCount,
    staticDatabaseLimit,
    autoDatabaseLimitStatus,
    autoDatabaseCount,
    autoDatabaseLimit,
}: IndexesPageLicenseLimitsProps) {
    return (
        <>
            {staticClusterLimitStatus !== "notReached" && (
                <Alert
                    color={staticClusterLimitStatus === "limitReached" ? "danger" : "warning"}
                    className="text-center mb-3"
                >
                    <Icon icon="cluster" />
                    Cluster {staticClusterLimitStatus === "limitReached" ? "has reached" : "is reaching"} the{" "}
                    <strong>maximum number of static indexes</strong> allowed per cluster by your license{" "}
                    <strong>
                        ({staticClusterCount}/{staticClusterLimit})
                    </strong>
                    <br /> Delete unused indexes or{" "}
                    <strong>
                        <a href={upgradeLicenseLink} target="_blank">
                            upgrade your license
                        </a>
                    </strong>
                </Alert>
            )}

            {autoClusterLimitStatus !== "notReached" && (
                <Alert
                    color={autoClusterLimitStatus === "limitReached" ? "danger" : "warning"}
                    className="text-center mb-3"
                >
                    <Icon icon="cluster" />
                    Cluster {autoClusterLimitStatus === "limitReached" ? "has reached" : "is reaching"} the{" "}
                    <strong>maximum number of auto indexes</strong> allowed per cluster by your license{" "}
                    <strong>
                        ({autoClusterCount}/{autoClusterLimit})
                    </strong>
                    <br /> Delete unused indexes or{" "}
                    <strong>
                        <a href={upgradeLicenseLink} target="_blank">
                            upgrade your license
                        </a>
                    </strong>
                </Alert>
            )}

            {staticDatabaseLimitStatus !== "notReached" && (
                <Alert
                    color={staticDatabaseLimitStatus === "limitReached" ? "danger" : "warning"}
                    className="text-center mb-3"
                >
                    <Icon icon="database" />
                    Database {staticDatabaseLimitStatus === "limitReached" ? "has reached" : "is reaching"} the{" "}
                    <strong>maximum number of static indexes</strong> allowed per database by your license{" "}
                    <strong>
                        ({staticDatabaseCount}/{staticDatabaseLimit})
                    </strong>
                    <br /> Delete unused indexes or{" "}
                    <strong>
                        <a href={upgradeLicenseLink} target="_blank">
                            upgrade your license
                        </a>
                    </strong>
                </Alert>
            )}

            {autoDatabaseLimitStatus !== "notReached" && (
                <Alert
                    color={autoDatabaseLimitStatus === "limitReached" ? "danger" : "warning"}
                    className="text-center mb-3"
                >
                    <Icon icon="database" />
                    Database {autoDatabaseLimitStatus === "limitReached" ? "has reached" : "is reaching"} the{" "}
                    <strong>maximum number of auto indexes</strong> allowed per database by your license{" "}
                    <strong>
                        ({autoDatabaseCount}/{autoDatabaseLimit})
                    </strong>
                    <br /> Delete unused indexes or{" "}
                    <strong>
                        <a href={upgradeLicenseLink} target="_blank">
                            upgrade your license
                        </a>
                    </strong>
                </Alert>
            )}
        </>
    );
}
