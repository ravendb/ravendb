import moment from "moment";
import genUtils from "common/generalUtils";
import { StudioTranslate } from "common/i18n/resources";
import { SelectOption } from "components/common/select/Select";
import { mapDestinationsFromDto } from "components/common/formDestinations/utils/formDestinationsMapsFromDto";
import { formDestinationsUtils } from "components/common/formDestinations/utils/formDestinationsUtils";
import { EditBackupTaskFormData, MIN_RETENTION_IN_SEC, SnapshotCompressionAlgorithm } from "./editBackupTaskValidation";

type PeriodicBackupConfiguration = Raven.Client.Documents.Operations.Backups.PeriodicBackupConfiguration;
type BackupConfiguration = Raven.Client.Documents.Operations.Backups.BackupConfiguration;
type BackupEncryptionSettings = Raven.Client.Documents.Operations.Backups.BackupEncryptionSettings;
type BackupType = Raven.Client.Documents.Operations.Backups.BackupType;
type BackupUploadMode = Raven.Client.Documents.Operations.Backups.BackupUploadMode;
type CompressionLevel = System.IO.Compression.CompressionLevel;
type EncryptionMode = Raven.Client.Documents.Operations.Backups.EncryptionMode;
type OngoingTaskState = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;

const DEFAULT_FULL_BACKUP_FREQUENCY = "0 2 * * 0";
const DEFAULT_INCREMENTAL_BACKUP_FREQUENCY = "0 2 * * *";

const UNKNOWN_SERVER_LIMITS: periodicBackupServerLimitsResponse = {
    LocalRootPath: null,
    AllowedAwsRegions: null,
    AllowedDestinations: null,
    EnableUnencryptedBackupForEncryptedDatabase: false,
};

type Translate = StudioTranslate<"editBackupTask">;

function getTaskStateOptions(t: Translate): SelectOption<OngoingTaskState>[] {
    return [
        { value: "Enabled", label: t("settings.stateEnabled") },
        { value: "Disabled", label: t("settings.stateDisabled") },
    ];
}

function getBackupUploadModeOptions(t: Translate): SelectOption<BackupUploadMode>[] {
    return [
        { value: "Default", label: t("settings.uploadModeDefault") },
        { value: "DirectUpload", label: t("settings.uploadModeDirect") },
    ];
}

function getCompressionAlgorithmOptions(t: Translate): SelectOption<SnapshotCompressionAlgorithm>[] {
    return [
        { value: "Default", label: t("settings.compressionAlgorithmDefault") },
        { value: "Zstd", label: "Zstd" },
        { value: "Deflate", label: "Deflate" },
    ];
}

function getCompressionLevelOptions(t: Translate): SelectOption<CompressionLevel>[] {
    return [
        { value: "Optimal", label: t("settings.compressionLevelOptimal") },
        { value: "Fastest", label: t("settings.compressionLevelFastest") },
        { value: "NoCompression", label: t("settings.compressionLevelNone") },
        { value: "SmallestSize", label: t("settings.compressionLevelSmallest") },
    ];
}

function getEncryptionModeOptions(t: Translate): SelectOption<EncryptionMode>[] {
    return [
        { value: "UseDatabaseKey", label: t("encryption.modeDatabaseKey") },
        { value: "UseProvidedKey", label: t("encryption.modeProvidedKey") },
    ];
}

function getBackupTypeLabel(backupType: BackupType, t: Translate): string | null {
    switch (backupType) {
        case "Backup":
            return t("type.backup.title");
        case "Snapshot":
            return t("type.snapshot.title");
        default:
            return null;
    }
}

type EncryptionFields = Pick<EditBackupTaskFormData, "isEncrypted" | "encryptionMode">;

type EncryptionLockReason = "snapshotOfEncryptedDatabase" | "snapshotOfUnencryptedDatabase" | "unencryptedDatabase";

interface EncryptionConstraints {
    forced: Partial<EncryptionFields>;
    lockReason: EncryptionLockReason | null;
}

function getEncryptionConstraints(backupType: BackupType, isDatabaseEncrypted: boolean): EncryptionConstraints {
    if (backupType === "Snapshot") {
        return {
            forced: { isEncrypted: isDatabaseEncrypted, encryptionMode: "UseDatabaseKey" },
            lockReason: isDatabaseEncrypted ? "snapshotOfEncryptedDatabase" : "snapshotOfUnencryptedDatabase",
        };
    }

    if (!isDatabaseEncrypted) {
        return {
            forced: { encryptionMode: "UseProvidedKey" },
            lockReason: "unencryptedDatabase",
        };
    }

    return { forced: {}, lockReason: null };
}

interface EncryptionRules {
    isToggleLocked: boolean;
    isModeLocked: boolean;
    lockReason: string | null;
}

function getEncryptionRules(backupType: BackupType, isDatabaseEncrypted: boolean, t: Translate): EncryptionRules {
    const { forced, lockReason } = getEncryptionConstraints(backupType, isDatabaseEncrypted);

    return {
        isToggleLocked: forced.isEncrypted != null,
        isModeLocked: forced.encryptionMode != null,
        lockReason: lockReason ? t(`encryption.${lockReason}`) : null,
    };
}

function getEncryptionForBackupType(
    backupType: BackupType,
    isDatabaseEncrypted: boolean,
    current: EncryptionFields
): EncryptionFields {
    const { forced } = getEncryptionConstraints(backupType, isDatabaseEncrypted);

    return { ...current, ...forced };
}

function mapEncryptionFromDto(
    dto: BackupEncryptionSettings,
    isDatabaseEncrypted: boolean
): Pick<
    EditBackupTaskFormData,
    | "isEncrypted"
    | "isUnencryptedBackupAllowed"
    | "encryptionMode"
    | "encryptionKey"
    | "originalEncryptionKey"
    | "isEncryptionKeySaved"
> {
    const defaultMode: EncryptionMode = isDatabaseEncrypted ? "UseDatabaseKey" : "UseProvidedKey";

    if (!dto?.EncryptionMode) {
        return {
            isEncrypted: isDatabaseEncrypted,
            isUnencryptedBackupAllowed: false,
            encryptionMode: defaultMode,
            encryptionKey: null,
            originalEncryptionKey: null,
            isEncryptionKeySaved: false,
        };
    }

    const isEncrypted = dto.EncryptionMode !== "None";
    const originalKey = isEncrypted ? (dto.Key ?? null) : null;

    return {
        isEncrypted,
        isUnencryptedBackupAllowed: !isEncrypted,
        encryptionMode: isEncrypted ? dto.EncryptionMode : defaultMode,
        encryptionKey: originalKey,
        originalEncryptionKey: originalKey,
        isEncryptionKeySaved: !!originalKey,
    };
}

function mapEncryptionToDto(formData: EditBackupTaskFormData): BackupEncryptionSettings {
    if (!formData.isEncrypted) {
        return { EncryptionMode: "None", Key: undefined };
    }

    return {
        EncryptionMode: formData.encryptionMode,
        Key: formData.encryptionMode === "UseProvidedKey" ? formData.encryptionKey : undefined,
    };
}

function stripRootPath(path: string, rootPath: string): string {
    if (rootPath && path?.startsWith(rootPath)) {
        return path.slice(rootPath.length);
    }
    return path;
}

interface GetDefaultValuesArgs {
    dto: PeriodicBackupConfiguration | null;
    isDatabaseEncrypted: boolean;
    isSharded: boolean;
    localRootPath: string;
}

function getDefaultValues({
    dto,
    isDatabaseEncrypted,
    isSharded,
    localRootPath,
}: GetDefaultValuesArgs): EditBackupTaskFormData {
    const backupType: BackupType = dto?.BackupType ?? (isSharded ? "Backup" : null);
    const destinations = mapDestinationsFromDto({
        LocalSettings: dto?.LocalSettings
            ? { ...dto.LocalSettings, FolderPath: stripRootPath(dto.LocalSettings.FolderPath, localRootPath) }
            : null,
        S3Settings: dto?.S3Settings,
        AzureSettings: dto?.AzureSettings,
        GoogleCloudSettings: dto?.GoogleCloudSettings,
        GlacierSettings: dto?.GlacierSettings,
        FtpSettings: dto?.FtpSettings,
    });

    const minimumBackupAgeInSec = dto?.RetentionPolicy?.MinimumBackupAgeToKeep
        ? genUtils.timeSpanToSeconds(dto.RetentionPolicy.MinimumBackupAgeToKeep)
        : MIN_RETENTION_IN_SEC;

    return {
        backupType,
        name: dto?.Name ?? null,
        state: dto?.Disabled ? "Disabled" : "Enabled",
        backupUploadMode: dto?.BackupUploadMode ?? "Default",
        snapshotCompressionAlgorithm: dto?.SnapshotSettings
            ? (dto.SnapshotSettings.CompressionAlgorithm ?? "Default")
            : "Zstd",
        snapshotCompressionLevel: dto?.SnapshotSettings?.CompressionLevel ?? "Fastest",
        isSnapshotExcludeIndexes: dto?.SnapshotSettings?.ExcludeIndexes ?? false,
        isSetResponsibleNode: !!dto?.MentorNode,
        responsibleNode: dto?.MentorNode ?? null,
        isPinResponsibleNode: dto?.PinToMentorNode ?? false,
        isSetMaxReadOpsPerSecond: dto?.MaxReadOpsPerSecond != null,
        maxReadOpsPerSecond: dto?.MaxReadOpsPerSecond ?? null,
        isSetRetentionPolicy: dto?.RetentionPolicy ? !dto.RetentionPolicy.Disabled : false,
        minimumBackupAgeToKeepInSec: minimumBackupAgeInSec,
        ...mapEncryptionFromDto(dto?.BackupEncryptionSettings, isDatabaseEncrypted),
        isFullBackupEnabled: !!dto?.FullBackupFrequency,
        fullBackupFrequency: dto?.FullBackupFrequency ?? DEFAULT_FULL_BACKUP_FREQUENCY,
        isIncrementalBackupEnabled: !!dto?.IncrementalBackupFrequency,
        incrementalBackupFrequency: dto?.IncrementalBackupFrequency ?? DEFAULT_INCREMENTAL_BACKUP_FREQUENCY,
        destinations: destinations.destinations,
    };
}

function mapToManualBackupDto(formData: EditBackupTaskFormData, localRootPath: string): BackupConfiguration {
    const destinations = formDestinationsUtils.mapAddedDestinationsToDto(formData.destinations);

    return {
        BackupType: formData.backupType,
        BackupUploadMode: formData.backupUploadMode,
        SnapshotSettings: {
            CompressionAlgorithm:
                formData.snapshotCompressionAlgorithm === "Default" ? null : formData.snapshotCompressionAlgorithm,
            CompressionLevel: formData.snapshotCompressionLevel,
            ExcludeIndexes: formData.isSnapshotExcludeIndexes,
        },
        BackupEncryptionSettings: mapEncryptionToDto(formData),
        MaxReadOpsPerSecond: formData.isSetMaxReadOpsPerSecond ? formData.maxReadOpsPerSecond : null,
        ...destinations,
        LocalSettings:
            localRootPath && destinations.LocalSettings?.FolderPath
                ? { ...destinations.LocalSettings, FolderPath: localRootPath + destinations.LocalSettings.FolderPath }
                : destinations.LocalSettings,
    };
}

function mapToPeriodicBackupDto(
    formData: EditBackupTaskFormData,
    taskId: number,
    localRootPath: string,
    createdAt: string | null
): PeriodicBackupConfiguration {
    return {
        ...mapToManualBackupDto(formData, localRootPath),
        TaskId: taskId ?? 0,
        Name: formData.name,
        Disabled: formData.state === "Disabled",
        MentorNode: formData.isSetResponsibleNode ? formData.responsibleNode : undefined,
        PinToMentorNode: formData.isSetResponsibleNode && formData.isPinResponsibleNode,
        FullBackupFrequency: formData.isFullBackupEnabled ? formData.fullBackupFrequency : null,
        IncrementalBackupFrequency: formData.isIncrementalBackupEnabled ? formData.incrementalBackupFrequency : null,
        RetentionPolicy: {
            Disabled: !formData.isSetRetentionPolicy,
            MinimumBackupAgeToKeep:
                formData.minimumBackupAgeToKeepInSec >= MIN_RETENTION_IN_SEC
                    ? genUtils.formatAsTimeSpan(formData.minimumBackupAgeToKeepInSec * 1000)
                    : null,
        },
        CreatedAt: createdAt,
    };
}

function getEncryptionKeyFileName(databaseName: string): string {
    return `Backup-key-of-${databaseName}-${moment().format("YYYY-MM-DD-HH-mm")}.txt`;
}

function getEncryptionKeyText(databaseName: string, key: string, t: Translate): string {
    const header = t("encryption.keyFileHeader", { databaseName, key });
    return `${header}\r\n\r\n${t("encryption.keyFilePurpose")}\r\n${t("encryption.keyFileWarning")}`;
}

export const editBackupTaskUtils = {
    UNKNOWN_SERVER_LIMITS,
    getTaskStateOptions,
    getBackupUploadModeOptions,
    getCompressionAlgorithmOptions,
    getCompressionLevelOptions,
    getEncryptionModeOptions,
    getBackupTypeLabel,
    getDefaultValues,
    getEncryptionRules,
    getEncryptionForBackupType,
    mapToManualBackupDto,
    mapToPeriodicBackupDto,
    getEncryptionKeyFileName,
    getEncryptionKeyText,
};
