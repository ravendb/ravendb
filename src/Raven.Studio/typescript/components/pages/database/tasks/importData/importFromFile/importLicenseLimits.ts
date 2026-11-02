type LicenseLimitKey = Extract<keyof LicenseStatus, `MaxNumberOf${string}`>;

export type LicenseLimitedImportItem = "subscriptions" | "customSorters" | "customAnalyzers";

interface LicenseLimitKeys {
    label: string;
    perDatabase: LicenseLimitKey;
    perCluster: LicenseLimitKey;
}

const licenseLimitKeys: Record<LicenseLimitedImportItem, LicenseLimitKeys> = {
    subscriptions: {
        label: "subscriptions",
        perDatabase: "MaxNumberOfSubscriptionsPerDatabase",
        perCluster: "MaxNumberOfSubscriptionsPerCluster",
    },
    customSorters: {
        label: "custom sorters",
        perDatabase: "MaxNumberOfCustomSortersPerDatabase",
        perCluster: "MaxNumberOfCustomSortersPerCluster",
    },
    customAnalyzers: {
        label: "custom analyzers",
        perDatabase: "MaxNumberOfCustomAnalyzersPerDatabase",
        perCluster: "MaxNumberOfCustomAnalyzersPerCluster",
    },
};

export function getLicenseLimitWarning(
    licenseStatus: LicenseStatus | null,
    item: LicenseLimitedImportItem
): string | null {
    const { label, perDatabase, perCluster } = licenseLimitKeys[item];
    const limits = [
        describeLimit(licenseStatus?.[perDatabase], "database"),
        describeLimit(licenseStatus?.[perCluster], "cluster"),
    ].filter(Boolean);

    if (limits.length === 0) {
        return null;
    }

    return `Your license limits ${label} to ${limits.join(" and ")}. Anything above the limit will fail to import.`;
}

function describeLimit(limit: number | null, scope: "database" | "cluster"): string | null {
    return limit > 0 ? `${limit} per ${scope}` : null;
}
