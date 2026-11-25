import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormSwitch } from "components/common/Form";
import { FormDestinations } from "components/common/formDestinations/utils/formDestinationsTypes";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { useFormContext } from "react-hook-form";
import { useStudioTranslation } from "hooks/useStudioTranslation";

type FieldBase = `destinations.${keyof FormDestinations["destinations"]}`;

interface OverrideConfigurationViaExternalScriptToggleProps {
    fieldBase: FieldBase;
}

export default function OverrideConfigurationViaExternalScriptToggle({
    fieldBase,
}: OverrideConfigurationViaExternalScriptToggleProps) {
    const { control } = useFormContext<FormDestinations>();
    const t = useStudioTranslation("formDestinations");

    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const isRestrictExternalScriptUsageForNonClusterAdmin = useAppSelector(
        databaseSelectors.isRestrictExternalScriptUsageForNonClusterAdmin
    );

    const isDisabled = !isClusterAdminOrClusterNode && isRestrictExternalScriptUsageForNonClusterAdmin;

    return (
        <ConditionalPopover
            conditions={{
                isActive: isDisabled,
                message: t("scriptNotAllowed"),
            }}
        >
            <FormSwitch
                control={control}
                name={`${fieldBase}.config.isOverrideConfig`}
                className="ms-3"
                color="secondary"
                disabled={isDisabled}
            >
                {t("overrideViaScript")}
            </FormSwitch>
        </ConditionalPopover>
    );
}
