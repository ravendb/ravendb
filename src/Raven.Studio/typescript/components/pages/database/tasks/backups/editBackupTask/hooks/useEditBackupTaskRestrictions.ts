import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { EditBackupTaskContext } from "../utils/editBackupTaskSteps";
import { useEditBackupTaskLicense } from "./useEditBackupTaskLicense";

export function useEditBackupTaskRestrictions(context: EditBackupTaskContext) {
    const license = useEditBackupTaskLicense(context.isManual);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const isPeriodicRestricted = !context.isManual && !license.hasPeriodicBackup;
    const canChangeBackupType = hasDatabaseAdminAccess && !isPeriodicRestricted;

    return {
        ...license,
        hasDatabaseAdminAccess,
        isPeriodicRestricted,
        canChangeBackupType,
        isCreationBlocked: !context.isEditing && !canChangeBackupType,
    };
}
