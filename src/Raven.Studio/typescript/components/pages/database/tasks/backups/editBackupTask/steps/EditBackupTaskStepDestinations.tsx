import Button from "react-bootstrap/Button";
import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { FormValidationMessage } from "components/common/Form";
import { useViewSheet, ViewSheet } from "components/common/splitView/ViewSheet";
import DestinationPanel from "components/common/formDestinations/DestinationPanel";
import DestinationEditorSheet from "components/common/formDestinations/DestinationEditorSheet";
import { DestinationType, FormDestinationData } from "components/common/formDestinations/utils/formDestinationsTypes";
import { formDestinationsUtils } from "components/common/formDestinations/utils/formDestinationsUtils";
import { AccessPopover } from "components/common/AccessPopover";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { EditBackupTaskFormData } from "../utils/editBackupTaskValidation";
import { EditBackupTaskContext } from "../utils/editBackupTaskSteps";

interface EditBackupTaskStepDestinationsProps {
    context: EditBackupTaskContext;
}

export default function EditBackupTaskStepDestinations({ context }: EditBackupTaskStepDestinationsProps) {
    const { control, setValue, getValues, trigger, formState } = useFormContext<EditBackupTaskFormData>();
    const destinations = useWatch({ control, name: "destinations" });
    const { open } = useViewSheet();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const t = useStudioTranslation("editBackupTask");

    const addedTypes = formDestinationsUtils.getAddedDestinationTypes(destinations);
    const unusedTypes = formDestinationsUtils.DESTINATION_TYPES.filter((type) => !addedTypes.includes(type));
    const hasAllowedUnusedType = unusedTypes.some((type) =>
        formDestinationsUtils.isDestinationAllowed(type, context.serverLimits.AllowedDestinations)
    );
    const destinationsError = formState.errors.destinations?.message;

    const setDestination = (type: DestinationType, data: FormDestinationData) => {
        setValue(`destinations.${type}`, data, { shouldDirty: true });
    };

    const removeDestination = (type: DestinationType) => {
        setDestination(type, formDestinationsUtils.DESTINATION_TYPE_INFO[type].defaultData);
    };

    const setDestinationState = (type: DestinationType, isEnabled: boolean) => {
        setDestination(type, { ...getValues(`destinations.${type}`), isEnabled });
        trigger("destinations");
    };

    const applyDestination = (
        type: DestinationType,
        data: FormDestinationData,
        previousType: DestinationType | null
    ) => {
        if (previousType && previousType !== type) {
            removeDestination(previousType);
        }
        setDestination(type, data);
        trigger("destinations");
    };

    const openEditor = (editedType: DestinationType | null) => {
        open({
            initialWidth: 640,
            minWidth: 480,
            component: (
                <ViewSheet key={_.uniqueId("destination-editor-")} className="h-100">
                    <DestinationEditorSheet
                        editedType={editedType}
                        destinations={getValues("destinations")}
                        allowedDestinations={context.serverLimits.AllowedDestinations}
                        allowedAwsRegions={context.serverLimits.AllowedAwsRegions}
                        localRootPath={context.serverLimits.LocalRootPath}
                        isAwsSessionTokenVisible={context.isManual}
                        isS3StorageClassVisible
                        onApply={applyDestination}
                    />
                </ViewSheet>
            ),
        });
    };

    return (
        <div className="vstack gap-2">
            {addedTypes.map((type) => (
                <DestinationPanel
                    key={type}
                    type={type}
                    isDisabled={!hasDatabaseAdminAccess}
                    allowedDestinations={context.serverLimits.AllowedDestinations}
                    onStateChange={context.isManual ? undefined : (isEnabled) => setDestinationState(type, isEnabled)}
                    onEdit={() => openEditor(type)}
                    onDelete={() => {
                        removeDestination(type);
                        trigger("destinations");
                    }}
                />
            ))}
            {unusedTypes.length > 0 && (
                <div className="edit-backup-task-add-destination d-flex justify-content-center p-4">
                    <AccessPopover
                        accessRequired="DatabaseAdmin"
                        conditions={{ isActive: !hasAllowedUnusedType, message: t("destinations.disabledByAdmin") }}
                    >
                        <Button
                            variant="outline-primary"
                            className="rounded-pill"
                            onClick={() => openEditor(null)}
                            disabled={!hasDatabaseAdminAccess || !hasAllowedUnusedType}
                        >
                            <Icon icon="add" />
                            {t("destinations.add")}
                        </Button>
                    </AccessPopover>
                </div>
            )}
            {destinationsError && <FormValidationMessage>{destinationsError}</FormValidationMessage>}
        </div>
    );
}
