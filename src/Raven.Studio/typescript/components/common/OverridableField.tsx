import { ReactNode } from "react";
import { Control, FieldPath, FieldValues, PathValue, UseFormSetValue, useWatch } from "react-hook-form";
import Form from "react-bootstrap/Form";
import { FormSwitch } from "components/common/Form";
import FieldLabel from "components/common/FieldLabel";
import { PopoverWithHoverProps } from "components/common/PopoverWithHover";
import useUniqueId from "components/hooks/useUniqueId";
import "./OverridableField.scss";

interface ResetOnDisable<TFieldValues extends FieldValues, TValueName extends FieldPath<TFieldValues>> {
    setValue: UseFormSetValue<TFieldValues>;
    valueName: TValueName;
    resetValueTo?: PathValue<TFieldValues, TValueName>;
}

export interface OverridableFieldRenderState {
    isOverridden: boolean;
    isDisabled: boolean;
    controlId: string;
}

interface OverridableFieldProps<
    TFieldValues extends FieldValues,
    TOverrideName extends FieldPath<TFieldValues>,
    TValueName extends FieldPath<TFieldValues>,
> {
    control: Control<TFieldValues>;
    overrideName: TOverrideName;
    label: ReactNode;
    tooltip?: ReactNode;
    tooltipPlacement?: PopoverWithHoverProps["placement"];
    disabled?: boolean;
    children: (state: OverridableFieldRenderState) => ReactNode;
    resetOnDisable?: ResetOnDisable<TFieldValues, TValueName>;
}

export default function OverridableField<
    TFieldValues extends FieldValues,
    TOverrideName extends FieldPath<TFieldValues>,
    TValueName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
>({
    control,
    overrideName,
    label,
    tooltip,
    tooltipPlacement,
    disabled,
    children,
    resetOnDisable,
}: OverridableFieldProps<TFieldValues, TOverrideName, TValueName>) {
    const isOverridden = !!useWatch({ control, name: overrideName });
    const controlId = useUniqueId("overridable-field-control-");
    const labelId = useUniqueId("overridable-field-label-");
    const switchLabelId = useUniqueId("overridable-field-switch-label-");

    return (
        <Form.Group controlId={controlId}>
            <FieldLabel id={labelId} tooltip={tooltip} tooltipPlacement={tooltipPlacement}>
                {label}
            </FieldLabel>
            <div className="d-flex flex-wrap align-items-start gap-2">
                <div className="overridable-field-control">
                    {children({ isOverridden, isDisabled: disabled || !isOverridden, controlId })}
                </div>
                <div className="overridable-field-switch">
                    <FormSwitch
                        control={control}
                        name={overrideName}
                        disabled={disabled}
                        aria-labelledby={`${switchLabelId} ${labelId}`}
                        afterChange={(isChecked) => {
                            if (!isChecked && resetOnDisable) {
                                const {
                                    setValue,
                                    valueName,
                                    resetValueTo = null as PathValue<TFieldValues, TValueName>,
                                } = resetOnDisable;
                                setValue(valueName, resetValueTo, { shouldValidate: true });
                            }
                        }}
                    >
                        <span id={switchLabelId}>Override</span>
                    </FormSwitch>
                </div>
            </div>
        </Form.Group>
    );
}
