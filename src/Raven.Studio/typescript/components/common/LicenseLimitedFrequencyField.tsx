import classNames from "classnames";
import { Control, FieldPath, FieldValues, useController } from "react-hook-form";
import appUrl from "common/appUrl";
import { ZERO_DURATION_PLACEHOLDER } from "components/common/DurationDropdownPicker";
import FieldLabel from "components/common/FieldLabel";
import { ControlledDurationPicker } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import useUniqueId from "components/hooks/useUniqueId";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface LicenseLimitedFrequencyFieldProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: Control<TFieldValues>;
    name: TName;
    label: string;
    minPeriodInHours: number;
    limitMessage: string;
    isBelowLimit: boolean;
    disabled: boolean;
}

export default function LicenseLimitedFrequencyField<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
>({
    control,
    name,
    label,
    minPeriodInHours,
    limitMessage,
    isBelowLimit,
    disabled,
}: LicenseLimitedFrequencyFieldProps<TFieldValues, TName>) {
    const t = useStudioTranslation("licenseLimitedFrequencyField");
    const labelId = useUniqueId("license-limited-frequency-label-");
    const { field, fieldState, formState } = useController({ control, name });

    return (
        <div>
            <FieldLabel id={labelId} className="d-block">
                {label}
            </FieldLabel>
            <ControlledDurationPicker
                field={field}
                fieldState={fieldState}
                isSubmitting={formState.isSubmitting}
                disabled={disabled || formState.isSubmitting}
                aria-labelledby={labelId}
                placeholder={ZERO_DURATION_PLACEHOLDER}
                showSeconds
            />
            <div
                className={classNames("d-flex flex-wrap align-items-center column-gap-1 mt-1 small", {
                    "item-disabled pe-none": disabled,
                })}
            >
                {isBelowLimit && !fieldState.invalid ? (
                    <span className="text-danger">
                        <Icon icon="warning" />
                        {limitMessage}
                    </span>
                ) : (
                    <span className="text-muted">
                        <StudioTrans
                            ns="licenseLimitedFrequencyField"
                            i18nKey="licenseMinimum"
                            components={{ strong: <strong /> }}
                            options={{ count: minPeriodInHours }}
                        />
                    </span>
                )}
                <a href={appUrl.forAbout()} aria-disabled={disabled} tabIndex={disabled ? -1 : undefined}>
                    {t("seeFullComparison")}
                </a>
            </div>
        </div>
    );
}
