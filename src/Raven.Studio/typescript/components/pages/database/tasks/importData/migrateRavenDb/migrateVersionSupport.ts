import {
    DocumentToggleKey,
    documentToggleKeys,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";
import { ImportRestriction } from "components/pages/database/tasks/importData/shared/importRestrictions";
import { AuthenticationMethod } from "components/pages/database/tasks/importData/migrateRavenDb/migrateRavenDbValidation";

type MajorVersion = Raven.Server.Smuggler.Migration.MajorVersion;

export interface SourceServerVersion {
    majorVersion: MajorVersion;
    buildVersion: number;
    productVersion: string;
    fullVersion: string;
}

const NEVER_MIGRATED_TOGGLES = [
    "isIncludeArtificialDocuments",
    "isIncludeArchivedDocuments",
    "isIncludeExpiredDocuments",
    "isIncludeDocumentsTombstones",
    "isIncludeCompareExchangeTombstones",
] as const satisfies readonly DocumentToggleKey[];

export type MigrateDocumentToggleKey = Exclude<DocumentToggleKey, (typeof NEVER_MIGRATED_TOGGLES)[number]>;

const LEGACY_UNSUPPORTED_TOGGLES: readonly MigrateDocumentToggleKey[] = [
    "isIncludeAttachments",
    "isIncludeCounters",
    "isIncludeTimeSeries",
    "isIncludeTimeSeriesDeletedRanges",
    "isIncludeConflicts",
    "isIncludeCompareExchange",
    "isIncludeSubscriptions",
];

const NEVER_MIGRATED_TOGGLE_SET = new Set<DocumentToggleKey>(NEVER_MIGRATED_TOGGLES);

const MIGRATE_DOCUMENT_TOGGLES = documentToggleKeys.filter(
    (key): key is MigrateDocumentToggleKey => !NEVER_MIGRATED_TOGGLE_SET.has(key)
);

export function isLegacyVersion(version: SourceServerVersion): boolean {
    return version.majorVersion === "V30" || version.majorVersion === "V35";
}

function isBuildAtLeast(version: SourceServerVersion, major: number, minor: number): boolean {
    if (isLegacyVersion(version)) {
        return false;
    }
    const { buildVersion } = version;
    return buildVersion >= major * 10000 + minor * 1000 || (buildVersion >= major * 10 + minor && buildVersion <= 99);
}

// mirrors the Importer on the server: deleted ranges came with 5.4.201 and 6.0.105
function hasTimeSeriesDeletedRanges(buildVersion: number): boolean {
    const isUnsupported =
        (buildVersion >= 40_000 && buildVersion <= 54_133) ||
        (buildVersion >= 60_000 && buildVersion <= 60_035) ||
        (buildVersion >= 40 && buildVersion < 54);
    return !isUnsupported;
}

export function getUnsupportedDocumentToggles(version: SourceServerVersion): MigrateDocumentToggleKey[] {
    if (isLegacyVersion(version)) {
        return [...LEGACY_UNSUPPORTED_TOGGLES];
    }

    const unsupported: MigrateDocumentToggleKey[] = [];
    if (!isBuildAtLeast(version, 4, 2)) {
        unsupported.push("isIncludeCounters");
    }
    if (!isBuildAtLeast(version, 5, 0)) {
        unsupported.push("isIncludeTimeSeries");
    }
    if (!hasTimeSeriesDeletedRanges(version.buildVersion)) {
        unsupported.push("isIncludeTimeSeriesDeletedRanges");
    }
    return unsupported;
}

export function getMigratableDocumentToggles(version: SourceServerVersion): MigrateDocumentToggleKey[] {
    const unsupported = getUnsupportedDocumentToggles(version);
    return MIGRATE_DOCUMENT_TOGGLES.filter(
        (key) => !unsupported.includes(key) && (key !== "isIncludeLegacyAttachments" || isLegacyVersion(version))
    );
}

export function formatServerVersion(version: SourceServerVersion | null): string {
    if (!version || version.majorVersion === "Unknown") {
        return "N/A";
    }
    const build = `(build: ${version.buildVersion})`;
    switch (version.majorVersion) {
        case "V2":
            return `2.x ${build}`;
        case "V30":
            return `3.0 ${build}`;
        case "V35":
            return `3.5 ${build}`;
        default:
            return version.fullVersion || version.productVersion;
    }
}

export function getUnsupportedVersionReason(version: SourceServerVersion): string {
    return `Not supported by source server version ${formatServerVersion(version)}`;
}

export function getVisibleDocumentToggles(version: SourceServerVersion | null): MigrateDocumentToggleKey[] {
    const isLegacy = !!version && isLegacyVersion(version);
    return MIGRATE_DOCUMENT_TOGGLES.filter((key) => key !== "isIncludeLegacyAttachments" || isLegacy);
}

export function getVersionToggleRestrictions(
    version: SourceServerVersion | null
): Partial<Record<DocumentToggleKey, ImportRestriction>> {
    if (!version) {
        return {};
    }
    const restriction = {
        reason: "sourceVersion",
        label: "Source server version",
        icon: "server",
        tooltip: getUnsupportedVersionReason(version),
    } satisfies ImportRestriction;

    return Object.fromEntries(getUnsupportedDocumentToggles(version).map((key) => [key, restriction]));
}

export function getEffectiveAuthenticationMethod(
    authenticationMethod: AuthenticationMethod,
    version: SourceServerVersion | null
): AuthenticationMethod {
    return version && isLegacyVersion(version) ? authenticationMethod : "none";
}
