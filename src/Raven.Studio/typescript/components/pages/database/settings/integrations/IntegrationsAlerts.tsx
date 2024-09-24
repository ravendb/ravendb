import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import React from "react";
import RichAlert from "components/common/RichAlert";

interface IntegrationsAlertsProps {
    isLicenseUpgradeRequired: boolean;
    isPostgreSqlSupportEnabled: boolean;
}

export default function IntegrationsAlerts(props: IntegrationsAlertsProps) {
    const { isLicenseUpgradeRequired, isPostgreSqlSupportEnabled } = props;

    const postgreSqlDocsLink = useRavenLink({ hash: "HDTCH7" });
    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    if (isLicenseUpgradeRequired) {
        return (
            <RichAlert variant="warning">
                <span>
                    To use this feature, your license must include either of the following features: PostgreSQL
                    integration or Power BI.
                </span>

                <div className="mt-1">
                    <a href={buyLink} target="_blank" className="btn btn-primary rounded-pill">
                        Licensing options <Icon icon="newtab" margin="ms-1" />
                    </a>
                </div>
            </RichAlert>
        );
    }

    if (!isLicenseUpgradeRequired && !isPostgreSqlSupportEnabled) {
        return (
            <RichAlert variant="warning" className="mt-3">
                PostgreSQL support must be explicitly enabled in your <code>settings.json</code> file. Learn more{" "}
                <a href={postgreSqlDocsLink}>here</a>.
            </RichAlert>
        );
    }

    return null;
}
