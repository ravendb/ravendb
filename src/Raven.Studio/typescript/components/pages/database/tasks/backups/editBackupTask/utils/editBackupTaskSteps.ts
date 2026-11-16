import { StudioTranslate } from "common/i18n/resources";
import { WizardStepsListItem } from "components/common/wizard/WizardStepsList";
import { CronServerErrors, EditBackupTaskFormData, editBackupTaskFieldsByStep } from "./editBackupTaskValidation";

export type EditBackupTaskStepId = "type" | "settings" | "schedule" | "destinations" | "summary";

export interface EditBackupTaskContext {
    isManual: boolean;
    isEditing: boolean;
    taskId: number | null;
    serverLimits: periodicBackupServerLimitsResponse;
    cronServerErrors: CronServerErrors;
    goToStep: (stepId: EditBackupTaskStepId) => void;
}

const ALL_STEP_IDS: EditBackupTaskStepId[] = ["type", "settings", "schedule", "destinations", "summary"];

export function getEditBackupTaskSteps(
    isManual: boolean,
    t: StudioTranslate<"editBackupTask">
): WizardStepsListItem<EditBackupTaskStepId>[] {
    return ALL_STEP_IDS.filter((id) => !isManual || id !== "schedule").map((id) => ({ id, title: t(`steps.${id}`) }));
}

export function getEditBackupTaskStepFields(stepId: EditBackupTaskStepId): (keyof EditBackupTaskFormData)[] {
    return stepId === "summary" ? [] : editBackupTaskFieldsByStep[stepId];
}
