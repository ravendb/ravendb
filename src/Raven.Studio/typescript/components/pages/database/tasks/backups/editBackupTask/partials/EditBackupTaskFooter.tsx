import Button from "react-bootstrap/Button";
import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { AccessPopover } from "components/common/AccessPopover";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { getAccessRequiredMessage } from "components/utils/accessUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { WizardStepsListItem } from "components/common/wizard/WizardStepsList";
import { EditBackupTaskFormData } from "../utils/editBackupTaskValidation";
import { EditBackupTaskContext, EditBackupTaskStepId, getEditBackupTaskStepFields } from "../utils/editBackupTaskSteps";
import { useEditBackupTaskRestrictions } from "../hooks/useEditBackupTaskRestrictions";

interface EditBackupTaskFooterProps {
    steps: WizardStepsListItem<EditBackupTaskStepId>[];
    currentStepId: EditBackupTaskStepId;
    context: EditBackupTaskContext;
    onCancel: () => void;
}

export default function EditBackupTaskFooter({ steps, currentStepId, context, onCancel }: EditBackupTaskFooterProps) {
    const { control, trigger, formState } = useFormContext<EditBackupTaskFormData>();
    const backupType = useWatch({ control, name: "backupType" });
    const { restrictionReason, hasDatabaseAdminAccess, isPeriodicRestricted, isCreationBlocked } =
        useEditBackupTaskRestrictions(context);
    const t = useStudioTranslation("editBackupTask");
    const commonT = useStudioTranslation("common");
    const createButtonLabel = context.isManual ? t("footer.createBackup") : t("footer.createBackupTask");

    const currentStepIdx = steps.findIndex((x) => x.id === currentStepId);
    const previousStep = steps[currentStepIdx - 1];
    const nextStep = steps[currentStepIdx + 1];

    const isNextDisabled = isCreationBlocked || (currentStepId === "type" && !backupType);

    const handleNext = async () => {
        const isValid = await trigger(getEditBackupTaskStepFields(currentStepId));
        if (isValid) {
            context.goToStep(nextStep.id);
        }
    };

    return (
        <div className="hstack gap-2 justify-content-between">
            <Button variant="outline-secondary" className="rounded-pill" onClick={onCancel}>
                {commonT("cancel")}
            </Button>
            <div className="hstack gap-2">
                {previousStep && (
                    <Button
                        variant="secondary"
                        className="rounded-pill"
                        onClick={() => context.goToStep(previousStep.id)}
                    >
                        <Icon icon="arrow-left" />
                        {commonT("back")}
                    </Button>
                )}
                {nextStep ? (
                    <ConditionalPopover
                        conditions={[
                            {
                                isActive: isCreationBlocked && !hasDatabaseAdminAccess,
                                message: getAccessRequiredMessage("DatabaseAdmin"),
                            },
                            {
                                isActive: isCreationBlocked && isPeriodicRestricted,
                                message: restrictionReason,
                            },
                        ]}
                        popoverPlacement="top"
                    >
                        <Button
                            variant="primary"
                            className="rounded-pill"
                            onClick={handleNext}
                            disabled={isNextDisabled}
                        >
                            {commonT("next")}
                            <Icon icon="arrow-right" margin="ms-1" />
                        </Button>
                    </ConditionalPopover>
                ) : (
                    <AccessPopover
                        accessRequired="DatabaseAdmin"
                        conditions={{ isActive: !!restrictionReason, message: restrictionReason }}
                        popoverPlacement="top"
                    >
                        <ButtonWithSpinner
                            type="submit"
                            variant="primary"
                            className="rounded-pill"
                            isSpinning={formState.isSubmitting}
                            disabled={
                                !hasDatabaseAdminAccess ||
                                !!restrictionReason ||
                                (context.isEditing && !formState.isDirty)
                            }
                            icon="save"
                        >
                            {context.isEditing ? commonT("save") : createButtonLabel}
                        </ButtonWithSpinner>
                    </AccessPopover>
                )}
            </div>
        </div>
    );
}
