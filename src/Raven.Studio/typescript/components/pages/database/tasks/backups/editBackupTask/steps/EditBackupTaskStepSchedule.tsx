import classNames from "classnames";
import { useFormContext, useWatch } from "react-hook-form";
import { FormSwitch, FormValidationMessage } from "components/common/Form";
import FormCronEditor, { CronServerValidationHandler } from "components/common/cronEditor/FormCronEditor";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { EditBackupTaskFormData } from "../utils/editBackupTaskValidation";
import { EditBackupTaskContext } from "../utils/editBackupTaskSteps";
import { useEditBackupTaskRestrictions } from "../hooks/useEditBackupTaskRestrictions";

interface EditBackupTaskStepScheduleProps {
    context: EditBackupTaskContext;
}

export default function EditBackupTaskStepSchedule({ context }: EditBackupTaskStepScheduleProps) {
    const { formState } = useFormContext<EditBackupTaskFormData>();
    const t = useStudioTranslation("editBackupTask");

    const scheduleError = formState.errors.isIncrementalBackupEnabled?.message;

    return (
        <div className="vstack gap-4">
            <ScheduleSection
                context={context}
                title={t("schedule.fullTitle")}
                description={t("schedule.fullDescription")}
                helpMessage={t("schedule.fullHelp")}
                enableLabel={t("schedule.fullEnableLabel")}
                enabledFieldName="isFullBackupEnabled"
                frequencyFieldName="fullBackupFrequency"
                isFullBackup
            />
            <ScheduleSection
                context={context}
                title={t("schedule.incrementalTitle")}
                description={t("schedule.incrementalDescription")}
                helpMessage={t("schedule.incrementalHelp")}
                enableLabel={t("schedule.incrementalEnableLabel")}
                enabledFieldName="isIncrementalBackupEnabled"
                frequencyFieldName="incrementalBackupFrequency"
                isFullBackup={false}
            />
            {scheduleError && <FormValidationMessage>{scheduleError}</FormValidationMessage>}
        </div>
    );
}

interface ScheduleSectionProps {
    context: EditBackupTaskContext;
    title: string;
    description: string;
    helpMessage: string;
    enableLabel: string;
    enabledFieldName: "isFullBackupEnabled" | "isIncrementalBackupEnabled";
    frequencyFieldName: "fullBackupFrequency" | "incrementalBackupFrequency";
    isFullBackup: boolean;
}

function ScheduleSection({
    context,
    title,
    description,
    helpMessage,
    enableLabel,
    enabledFieldName,
    frequencyFieldName,
    isFullBackup,
}: ScheduleSectionProps) {
    const { control, trigger } = useFormContext<EditBackupTaskFormData>();
    const isEnabled = useWatch({ control, name: enabledFieldName });
    const { hasDatabaseAdminAccess } = useEditBackupTaskRestrictions(context);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const handleServerValidation: CronServerValidationHandler = (expression, errorMessage) => {
        if (errorMessage) {
            context.cronServerErrors.set(expression, errorMessage);
            trigger(frequencyFieldName);
        } else {
            context.cronServerErrors.delete(expression);
        }
    };

    return (
        <section className="vstack gap-3">
            <div className="vstack gap-1">
                <div className="hstack gap-2">
                    <h3 className="mb-0">{title}</h3>
                    <PopoverWithHoverWrapper message={helpMessage}>
                        <Icon icon="help" margin="m-0" className="text-muted" />
                    </PopoverWithHoverWrapper>
                    <FormSwitch
                        control={control}
                        name={enabledFieldName}
                        rules={isFullBackup ? { deps: ["isIncrementalBackupEnabled"] } : undefined}
                        aria-label={enableLabel}
                        disabled={!hasDatabaseAdminAccess}
                    />
                </div>
                <p className="mb-0">{description}</p>
            </div>
            <div
                className={classNames("panel-bg-1 border border-color-light rounded-2 p-3", {
                    "item-disabled": !isEnabled,
                })}
                inert={!isEnabled}
            >
                <FormCronEditor
                    control={control}
                    name={frequencyFieldName}
                    databaseName={context.isEditing ? databaseName : undefined}
                    taskId={context.taskId ?? undefined}
                    isFullBackup={context.isEditing ? isFullBackup : undefined}
                    onServerValidation={handleServerValidation}
                    isDisabled={!hasDatabaseAdminAccess}
                    isSummaryHidden={!isEnabled}
                />
            </div>
        </section>
    );
}
