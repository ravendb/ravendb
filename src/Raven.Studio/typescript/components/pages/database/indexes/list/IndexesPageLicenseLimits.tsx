import { LicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import RichAlert from "components/common/RichAlert";

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
                <RichAlert
                    variant={staticClusterLimitStatus === "limitReached" ? "danger" : "warning"}
                    icon="cluster"
                    iconAddon="warning"
                    className="mb-3"
                >
                    Cluster {staticClusterLimitStatus === "limitReached" ? "has reached" : "is reaching"} the{" "}
                    <strong>maximum number of static indexes</strong> allowed per cluster by your license{" "}
                    <strong>
                        ({staticClusterCount}/{staticClusterLimit})
                    </strong>
                    . Delete unused indexes or{" "}
                    <strong>
                        <a href={upgradeLicenseLink} target="_blank" className="no-decor">
                            upgrade your license
                        </a>
                    </strong>
                </RichAlert>
            )}

            {autoClusterLimitStatus !== "notReached" && (
                <RichAlert
                    variant={autoClusterLimitStatus === "limitReached" ? "danger" : "warning"}
                    icon="cluster"
                    iconAddon="warning"
                    className="mb-3"
                >
                    Cluster {autoClusterLimitStatus === "limitReached" ? "has reached" : "is reaching"} the{" "}
                    <strong>maximum number of auto indexes</strong> allowed per cluster by your license{" "}
                    <strong>
                        ({autoClusterCount}/{autoClusterLimit})
                    </strong>
                    . Delete unused indexes or{" "}
                    <strong>
                        <a href={upgradeLicenseLink} target="_blank" className="no-decor">
                            upgrade your license
                        </a>
                    </strong>
                </RichAlert>
            )}

            {staticDatabaseLimitStatus !== "notReached" && (
                <RichAlert
                    variant={staticDatabaseLimitStatus === "limitReached" ? "danger" : "warning"}
                    icon="database"
                    iconAddon="warning"
                    className="mb-3"
                >
                    Database {staticDatabaseLimitStatus === "limitReached" ? "has reached" : "is reaching"} the{" "}
                    <strong>maximum number of static indexes</strong> allowed per database by your license{" "}
                    <strong>
                        ({staticDatabaseCount}/{staticDatabaseLimit})
                    </strong>
                    . Delete unused indexes or{" "}
                    <strong>
                        <a href={upgradeLicenseLink} target="_blank" className="no-decor">
                            upgrade your license
                        </a>
                    </strong>
                </RichAlert>
            )}

            {autoDatabaseLimitStatus !== "notReached" && (
                <RichAlert
                    variant={autoDatabaseLimitStatus === "limitReached" ? "danger" : "warning"}
                    icon="database"
                    iconAddon="warning"
                    className="mb-3"
                >
                    Database {autoDatabaseLimitStatus === "limitReached" ? "has reached" : "is reaching"} the{" "}
                    <strong>maximum number of auto indexes</strong> allowed per database by your license{" "}
                    <strong>
                        ({autoDatabaseCount}/{autoDatabaseLimit})
                    </strong>
                    . Delete unused indexes or{" "}
                    <strong>
                        <a href={upgradeLicenseLink} target="_blank" className="no-decor">
                            upgrade your license
                        </a>
                    </strong>
                </RichAlert>
            )}
        </>
    );
}
