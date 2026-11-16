import { AboutViewHeading } from "components/common/AboutView";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { EditBackupTaskStepId } from "../utils/editBackupTaskSteps";
import { EditPeriodicBackupTaskInfoHub } from "./EditPeriodicBackupTaskInfoHub";
import { EditManualBackupTaskInfoHub } from "./EditManualBackupTaskInfoHub";

interface EditBackupTaskHeaderProps {
    stepId: EditBackupTaskStepId;
    isManual: boolean;
}

export default function EditBackupTaskHeader({ stepId, isManual }: EditBackupTaskHeaderProps) {
    const hasPeriodicBackup = useAppSelector(licenseSelectors.statusValue("HasPeriodicBackup"));
    const t = useStudioTranslation("editBackupTask");

    return (
        <>
            <div className="hstack gap-3 justify-content-between align-items-start">
                <AboutViewHeading
                    title={t(`${stepId}.title`)}
                    icon="backup"
                    marginBottom={2}
                    licenseBadgeText={!isManual && !hasPeriodicBackup ? "Professional +" : null}
                />
                {isManual ? <EditManualBackupTaskInfoHub /> : <EditPeriodicBackupTaskInfoHub />}
            </div>
            <p className="edit-backup-task-description mb-4">
                <StudioTrans ns="editBackupTask" i18nKey={`${stepId}.description`} components={{ br: <br /> }} />
            </p>
        </>
    );
}
