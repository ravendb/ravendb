import {
    DocumentToggleKey,
    documentToggleKeys,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";
import { ImportRestriction } from "components/pages/database/tasks/importData/shared/importRestrictions";

type MajorVersion = Raven.Server.Smuggler.Migration.MajorVersion;

export interface SourceServerVersion {
    majorVersion: MajorVersion;
    buildVersion: number;
    productVersion: string;
    fullVersion: string;
}

const NEVER_MIGRATED_TOGGLES: readonly DocumentToggleKey[] = [
    "isIncludeArtificialDocuments",
    "isIncludeArchivedDocuments",
    "isIncludeExpiredDocuments",
    "isIncludeDocumentsTombstones",
    "isIncludeCompareExchangeTombstones",
];

const LEGACY_UNSUPPORTED_TOGGLES: readonly DocumentToggleKey[] = [
    "isIncludeAttachments",
    "isIncludeCounters",
    "isIncludeTimeSeries",
    "isIncludeTimeSeriesDeletedRanges",
    "isIncludeConflicts",
    "isIncludeCompareExchange",
    "isIncludeSubscriptions",
];

export const MIGRATE_DOCUMENT_TOGGLES: readonly DocumentToggleKey[] = documentToggleKeys.filter(
    (key) => !NEVER_MIGRATED_TOGGLES.includes(key)
);

export function isLegacyVersion(version: SourceServerVersion): boolean {
    return version.majorVersion === "V30" || version.majorVersion === "V35";
}

export function isBuildAtLeast(version: SourceServerVersion, major: number, minor: number): boolean {
    if (isLegacyVersion(version)) {
        return false;
    }
    const { buildVersion } = version;
    return buildVersion >= major * 10000 + minor * 1000 || (buildVersion >= major * 10 + minor && buildVersion <= 99);
}

export function getUnsupportedDocumentToggles(version: SourceServerVersion): DocumentToggleKey[] {
    if (isLegacyVersion(version)) {
        return [...LEGACY_UNSUPPORTED_TOGGLES];
    }

    const unsupported: DocumentToggleKey[] = [];
    if (!isBuildAtLeast(version, 4, 2)) {
        unsupported.push("isIncludeCounters");
    }
    if (!isBuildAtLeast(version, 5, 0)) {
        unsupported.push("isIncludeTimeSeries");
    }
    if (!isBuildAtLeast(version, 6, 0)) {
        unsupported.push("isIncludeTimeSeriesDeletedRanges");
    }
    return unsupported;
}

export function getMigratableDocumentToggles(version: SourceServerVersion): DocumentToggleKey[] {
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

export function getVisibleDocumentToggles(version: SourceServerVersion | null): DocumentToggleKey[] {
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
