import { useMemo } from "react";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import { DatabaseSettingKey } from "./importFromFileValidation";

export interface RestrictedImportFeature {
    settingKey: DatabaseSettingKey;
    label: string;
    // lowest license tier that includes the feature (per the availability matrix in LicenseDetails.tsx)
    licenseRequired: LicenseBadgeText;
}

export function useImportLicenseRestrictions(): {
    restrictedFeatures: RestrictedImportFeature[];
    isSettingRestricted: (key: DatabaseSettingKey) => boolean;
    getRestrictionTooltip: (key: DatabaseSettingKey) => string | null;
    getLicenseRequired: (key: DatabaseSettingKey) => LicenseBadgeText | null;
} {
    const hasDocumentsCompression = useAppSelector(licenseSelectors.statusValue("HasDocumentsCompression"));
    const hasDataArchival = useAppSelector(licenseSelectors.statusValue("HasDataArchival"));
    const hasTimeSeriesRollupsAndRetention = useAppSelector(
        licenseSelectors.statusValue("HasTimeSeriesRollupsAndRetention")
    );
    const hasPostgreSqlIntegration = useAppSelector(licenseSelectors.statusValue("HasPostgreSqlIntegration"));
    const hasClientConfiguration = useAppSelector(licenseSelectors.statusValue("HasClientConfiguration"));

    return useMemo(() => {
        const restrictedFeatures: RestrictedImportFeature[] = [];

        if (!hasDocumentsCompression) {
            restrictedFeatures.push({
                settingKey: "documentsCompression",
                label: "Documents Compression",
                licenseRequired: "Enterprise",
            });
        }
        if (!hasDataArchival) {
            restrictedFeatures.push({
                settingKey: "dataArchival",
                label: "Data Archival",
                licenseRequired: "Enterprise",
            });
        }
        if (!hasTimeSeriesRollupsAndRetention) {
            restrictedFeatures.push({
                settingKey: "timeSeries",
                label: "Time Series Configuration",
                licenseRequired: "Professional +",
            });
        }
        if (!hasPostgreSqlIntegration) {
            restrictedFeatures.push({
                settingKey: "postgreSqlIntegration",
                label: "PostgreSQL Integration",
                licenseRequired: "Enterprise",
            });
        }
        if (!hasClientConfiguration) {
            restrictedFeatures.push({
                settingKey: "client",
                label: "Client Configuration",
                licenseRequired: "Professional +",
            });
        }

        const isSettingRestricted = (key: DatabaseSettingKey) =>
            restrictedFeatures.some((feature) => feature.settingKey === key);

        const getRestrictionTooltip = (key: DatabaseSettingKey) => {
            const feature = restrictedFeatures.find((f) => f.settingKey === key);
            return feature
                ? `Data created with ${feature.label} won't be imported - this feature isn't included in your license`
                : null;
        };

        const getLicenseRequired = (key: DatabaseSettingKey) =>
            restrictedFeatures.find((f) => f.settingKey === key)?.licenseRequired ?? null;

        return { restrictedFeatures, isSettingRestricted, getRestrictionTooltip, getLicenseRequired };
    }, [
        hasDocumentsCompression,
        hasDataArchival,
        hasTimeSeriesRollupsAndRetention,
        hasPostgreSqlIntegration,
        hasClientConfiguration,
    ]);
}
