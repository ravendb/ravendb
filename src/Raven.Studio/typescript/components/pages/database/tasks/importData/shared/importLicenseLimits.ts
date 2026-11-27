import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("importData");

type LicenseLimitKey = Extract<keyof LicenseStatus, `MaxNumberOf${string}`>;

export type LicenseLimitedImportItem = "subscriptions" | "customSorters" | "customAnalyzers";

interface LicenseLimitKeys {
    perDatabase: LicenseLimitKey;
    perCluster: LicenseLimitKey;
}

const licenseLimitKeys: Record<LicenseLimitedImportItem, LicenseLimitKeys> = {
    subscriptions: {
        perDatabase: "MaxNumberOfSubscriptionsPerDatabase",
        perCluster: "MaxNumberOfSubscriptionsPerCluster",
    },
    customSorters: {
        perDatabase: "MaxNumberOfCustomSortersPerDatabase",
        perCluster: "MaxNumberOfCustomSortersPerCluster",
    },
    customAnalyzers: {
        perDatabase: "MaxNumberOfCustomAnalyzersPerDatabase",
        perCluster: "MaxNumberOfCustomAnalyzersPerCluster",
    },
};

export function getLicenseLimitWarning(
    licenseStatus: LicenseStatus | null,
    item: LicenseLimitedImportItem
): string | null {
    const { perDatabase, perCluster } = licenseLimitKeys[item];
    const perDatabaseLimit = licenseStatus?.[perDatabase];
    const perClusterLimit = licenseStatus?.[perCluster];
    const hasDatabaseLimit = perDatabaseLimit > 0;
    const hasClusterLimit = perClusterLimit > 0;

    if (!hasDatabaseLimit && !hasClusterLimit) {
        return null;
    }

    const context = hasDatabaseLimit && hasClusterLimit ? "both" : hasDatabaseLimit ? "database" : "cluster";

    return t("restrictions.licenseLimit", {
        context,
        items: t(`restrictions.licenseLimitItems.${item}`),
        perDatabase: perDatabaseLimit,
        perCluster: perClusterLimit,
    });
}
