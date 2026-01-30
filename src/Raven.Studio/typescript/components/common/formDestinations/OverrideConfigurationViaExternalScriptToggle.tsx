import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormSwitch } from "components/common/Form";
import { FormDestinations } from "components/common/formDestinations/utils/formDestinationsTypes";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import tasksCommonContent from "models/database/tasks/tasksCommonContent";
import { useMemo } from "react";
import { useFormContext } from "react-hook-form";

type FieldBase = `destinations.${keyof FormDestinations["destinations"]}`;

interface OverrideConfigurationViaExternalScriptToggleProps {
    fieldBase: FieldBase;
}

export default function OverrideConfigurationViaExternalScriptToggle({
    fieldBase,
}: OverrideConfigurationViaExternalScriptToggleProps) {
    const { control } = useFormContext<FormDestinations>();

    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const isRestrictExternalScriptUsageForNonClusterAdmin = useAppSelector(
        databaseSelectors.isRestrictExternalScriptUsageForNonClusterAdmin
    );

    const disabledReason = useMemo(() => {
        if (!isClusterAdminOrClusterNode && isRestrictExternalScriptUsageForNonClusterAdmin) {
            return tasksCommonContent.externalScriptNotAllowedForNonClusterAdmins;
        }

        return null;
    }, [isClusterAdminOrClusterNode, isRestrictExternalScriptUsageForNonClusterAdmin]);

    const isDisabled = !!disabledReason;

    return (
        <ConditionalPopover
            conditions={{
                isActive: isDisabled,
                message: disabledReason,
            }}
        >
            <FormSwitch
                control={control}
                name={`${fieldBase}.config.isOverrideConfig`}
                className="ms-3"
                color="secondary"
                disabled={isDisabled}
            >
                Override configuration via external script
            </FormSwitch>
        </ConditionalPopover>
    );
}
