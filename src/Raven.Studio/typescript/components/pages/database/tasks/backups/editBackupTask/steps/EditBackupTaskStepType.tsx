import { useFormContext, useWatch } from "react-hook-form";
import SelectableCard from "components/common/SelectableCard";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import RichAlert from "components/common/RichAlert";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { EditBackupTaskFormData } from "../utils/editBackupTaskValidation";
import { editBackupTaskUtils } from "../utils/editBackupTaskUtils";
import { EditBackupTaskContext } from "../utils/editBackupTaskSteps";
import { useEditBackupTaskRestrictions } from "../hooks/useEditBackupTaskRestrictions";

type BackupType = Raven.Client.Documents.Operations.Backups.BackupType;

interface EditBackupTaskStepTypeProps {
    context: EditBackupTaskContext;
}

export default function EditBackupTaskStepType({ context }: EditBackupTaskStepTypeProps) {
    const { control, setValue, getValues } = useFormContext<EditBackupTaskFormData>();
    const backupType = useWatch({ control, name: "backupType" });
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const { hasSnapshotBackups, isPeriodicRestricted, canChangeBackupType } = useEditBackupTaskRestrictions(context);
    const t = useStudioTranslation("editBackupTask");

    const selectBackupType = (type: BackupType) => {
        const encryption = editBackupTaskUtils.getEncryptionForBackupType(type, db.isEncrypted, {
            isEncrypted: getValues("isEncrypted"),
            encryptionMode: getValues("encryptionMode"),
        });

        setValue("backupType", type, { shouldDirty: true, shouldValidate: true });
        setValue("isEncrypted", encryption.isEncrypted, { shouldDirty: true });
        setValue("encryptionMode", encryption.encryptionMode, { shouldDirty: true });
    };

    return (
        <div>
            {isPeriodicRestricted && (
                <RichAlert variant="info" className="edit-backup-task-alert mb-3">
                    {t("type.periodicNotLicensed")}
                </RichAlert>
            )}
            <div role="radiogroup" aria-label={t("type.listLabel")} className="edit-backup-task-type-list vstack gap-2">
                <SelectableCard
                    icon="backup"
                    title={t("type.backup.title")}
                    description={t("type.backup.description")}
                    isSelected={backupType === "Backup"}
                    onClick={() => selectBackupType("Backup")}
                    isDisabled={!canChangeBackupType}
                    popoverMessage={
                        <ul className="mb-0 ps-3">
                            <li>{t("type.backup.storageInfo")}</li>
                            <li>{t("type.backup.fullBackupComparisonInfo")}</li>
                            <li>{t("type.backup.indexesInfo")}</li>
                        </ul>
                    }
                />
                <ConditionalPopover
                    className="w-100"
                    conditions={[
                        {
                            isActive: db.isSharded,
                            message: t("type.snapshot.notSupportedInSharded"),
                        },
                        {
                            isActive: !hasSnapshotBackups,
                            message: t("license.noSnapshotBackups"),
                        },
                    ]}
                >
                    <SelectableCard
                        className="w-100"
                        icon="snapshot-backup"
                        title={t("type.snapshot.title")}
                        description={t("type.snapshot.description")}
                        isSelected={backupType === "Snapshot"}
                        onClick={() => selectBackupType("Snapshot")}
                        isDisabled={!canChangeBackupType || db.isSharded || !hasSnapshotBackups}
                        badge={
                            !hasSnapshotBackups && (
                                <LicenseRestrictedBadge licenseRequired="Enterprise" className="ms-2" />
                            )
                        }
                        popoverMessage={
                            <ul className="mb-0 ps-3">
                                <li>{t("type.snapshot.restoreInfo")}</li>
                                <li>{t("type.snapshot.indexDataInfo")}</li>
                                <li>{t("type.snapshot.shardingInfo")}</li>
                            </ul>
                        }
                    />
                </ConditionalPopover>
            </div>
        </div>
    );
}
