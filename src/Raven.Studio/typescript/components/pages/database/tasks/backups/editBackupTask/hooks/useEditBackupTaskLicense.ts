import { useFormContext, useWatch } from "react-hook-form";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { formDestinationsUtils } from "components/common/formDestinations/utils/formDestinationsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { EditBackupTaskFormData } from "../utils/editBackupTaskValidation";

export function useEditBackupTaskLicense(isManual: boolean) {
    const { control } = useFormContext<EditBackupTaskFormData>();
    const [backupType, isEncrypted, destinations] = useWatch({
        control,
        name: ["backupType", "isEncrypted", "destinations"],
    });
    const t = useStudioTranslation("editBackupTask");

    const hasPeriodicBackup = useAppSelector(licenseSelectors.statusValue("HasPeriodicBackup"));
    const hasSnapshotBackups = useAppSelector(licenseSelectors.statusValue("HasSnapshotBackups"));
    const hasCloudBackups = useAppSelector(licenseSelectors.statusValue("HasCloudBackups"));
    const hasEncryptedBackups = useAppSelector(licenseSelectors.statusValue("HasEncryptedBackups"));

    const hasCloudDestination = formDestinationsUtils
        .getEnabledDestinationTypes(destinations)
        .some((type) => formDestinationsUtils.DESTINATION_TYPE_INFO[type].isCloud);

    const restrictionReason = (() => {
        if (!isManual && !hasPeriodicBackup) {
            return t("license.noPeriodicBackups");
        }
        if (backupType === "Snapshot" && !hasSnapshotBackups) {
            return t("license.noSnapshotBackups");
        }
        if (hasCloudDestination && !hasCloudBackups) {
            return t("license.noCloudBackups");
        }
        if (isEncrypted && !hasEncryptedBackups) {
            return t("license.noEncryptedBackups");
        }
        return null;
    })();

    return {
        hasPeriodicBackup,
        hasSnapshotBackups,
        restrictionReason,
    };
}
