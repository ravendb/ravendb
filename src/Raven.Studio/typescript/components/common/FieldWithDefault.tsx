import { ReactNode } from "react";
import { Control, FieldPath, FieldValues, PathValue, useController } from "react-hook-form";
import { omit, pick } from "lodash";
import classNames from "classnames";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import { GroupBase } from "react-select";
import FieldLabel from "components/common/FieldLabel";
import {
    ControlledDurationPicker,
    ControlledFieldProps,
    ControlledInput,
    ControlledSelect,
    FormDurationPickerProps,
    FormInputProps,
    FormSelectProps,
} from "components/common/Form";
import { Icon } from "components/common/Icon";
import { PopoverWithHoverProps } from "components/common/PopoverWithHover";
import useUniqueId from "components/hooks/useUniqueId";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import "./FieldWithDefault.scss";

interface FieldWithDefaultProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: Control<TFieldValues>;
    name: TName;
    label: ReactNode;
    tooltip?: ReactNode;
    tooltipPlacement?: PopoverWithHoverProps["placement"];
    disabled?: boolean;
    afterChange?: (value: PathValue<TFieldValues, TName>) => void;
    optimalValue?: PathValue<TFieldValues, TName>;
    optimalValueLabel?: string;
    optimalValueHint?: ReactNode;
    defaultValueLabel?: string;
}

const fieldWithDefaultPropNames = Object.keys({
    control: true,
    name: true,
    label: true,
    tooltip: true,
    tooltipPlacement: true,
    disabled: true,
    afterChange: true,
    optimalValue: true,
    optimalValueLabel: true,
    optimalValueHint: true,
    defaultValueLabel: true,
} satisfies Record<keyof FieldWithDefaultProps<FieldValues, string>, true>) as (keyof FieldWithDefaultProps<
    FieldValues,
    string
>)[];

export function FormInputWithDefault<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FieldWithDefaultProps<TFieldValues, TName> & { defaultValueLabel: string } & Omit<
            FormInputProps,
            "id" | "value" | "onChange" | "disabled" | "size" | "placeholder"
        >
) {
    const { fieldProps, inputProps } = splitProps(props);

    return (
        <FieldWithDefault<TFieldValues, TName> {...fieldProps}>
            {({ binding, isDisabled, defaultPlaceholder }) => (
                <ControlledInput {...binding} {...inputProps} placeholder={defaultPlaceholder} disabled={isDisabled} />
            )}
        </FieldWithDefault>
    );
}

export function FormSelectWithDefault<
    Option,
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
    Group extends GroupBase<Option> = GroupBase<Option>,
>(
    props: FieldWithDefaultProps<TFieldValues, TName> & { defaultValueLabel: string } & Omit<
            FormSelectProps<Option, false, Group>,
            "inputId" | "value" | "onChange" | "isDisabled" | "isMulti" | "placeholder"
        >
) {
    const { fieldProps, inputProps } = splitProps(props);

    return (
        <FieldWithDefault<TFieldValues, TName> {...fieldProps}>
            {({ binding, isDisabled, controlId, defaultPlaceholder }) => (
                <ControlledSelect
                    {...binding}
                    {...inputProps}
                    placeholder={defaultPlaceholder}
                    isDisabled={isDisabled}
                    inputId={controlId}
                />
            )}
        </FieldWithDefault>
    );
}

export function FormDurationPickerWithDefault<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: Omit<FieldWithDefaultProps<TFieldValues, TName>, "defaultValueLabel"> &
        Omit<FormDurationPickerProps, "id" | "disabled" | "placeholder" | "aria-labelledby" | "defaultTotalSeconds"> & {
            defaultTotalSeconds: number;
        }
) {
    const { fieldProps, inputProps } = splitProps(props);

    return (
        <FieldWithDefault<TFieldValues, TName> {...fieldProps}>
            {({ binding, isDisabled, controlId, labelId }) => (
                <ControlledDurationPicker
                    {...binding}
                    {...inputProps}
                    id={controlId}
                    disabled={isDisabled}
                    aria-labelledby={labelId}
                />
            )}
        </FieldWithDefault>
    );
}

interface FieldWithDefaultRenderState<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    binding: ControlledFieldProps<TFieldValues, TName>;
    isDisabled: boolean;
    controlId: string;
    labelId: string;
    defaultPlaceholder: string;
}

function FieldWithDefault<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    control,
    name,
    label,
    tooltip,
    tooltipPlacement,
    disabled,
    afterChange,
    optimalValue,
    optimalValueLabel = String(optimalValue),
    optimalValueHint,
    defaultValueLabel,
    children,
}: FieldWithDefaultProps<TFieldValues, TName> & {
    children: (state: FieldWithDefaultRenderState<TFieldValues, TName>) => ReactNode;
}) {
    const t = useStudioTranslation("fieldWithDefault");
    const commonT = useStudioTranslation("common");
    const { field, fieldState, formState } = useController({ control, name });

    const controlId = useUniqueId("field-with-default-control-");
    const labelId = useUniqueId("field-with-default-label-");

    const isDefault = field.value == null;
    const hasOptimalValue = optimalValue != null;
    const isOptimal = hasOptimalValue && field.value === optimalValue;
    const isOptimalSuggested = hasOptimalValue && !disabled && !isOptimal && !fieldState.invalid;

    const changeValue = (value: PathValue<TFieldValues, TName>) => {
        const newValue: PathValue<TFieldValues, TName> = value == null || value === "" ? null : value;
        field.onChange(newValue);
        afterChange?.(newValue);
    };

    const binding: ControlledFieldProps<TFieldValues, TName> = {
        field: { ...field, onChange: changeValue },
        fieldState,
        isSubmitting: formState.isSubmitting,
    };

    return (
        <Form.Group controlId={controlId}>
            <div className="field-with-default-label-row">
                <div className="d-flex align-items-center gap-2">
                    <FieldLabel id={labelId} tooltip={tooltip} tooltipPlacement={tooltipPlacement} className="mb-0">
                        {label}
                    </FieldLabel>
                    {isOptimal && (
                        <Badge bg="faded-primary" pill>
                            {t("optimal")}
                        </Badge>
                    )}
                </div>
                {!isDefault && !disabled && (
                    <Button
                        variant="link"
                        size="sm"
                        className="p-0 border-0"
                        onClick={() => changeValue(null)}
                        disabled={formState.isSubmitting}
                        aria-describedby={labelId}
                    >
                        <Icon icon="reset" />
                        {t("resetToDefault")}
                    </Button>
                )}
            </div>
            <div className={classNames({ "field-with-default-has-hint": isOptimalSuggested })}>
                {children({
                    binding,
                    isDisabled: disabled || formState.isSubmitting,
                    controlId,
                    labelId,
                    defaultPlaceholder: commonT("defaultPlaceholder", { value: defaultValueLabel }),
                })}
                {isOptimalSuggested && (
                    <div className="field-with-default-hint column-gap-1 font-size-10 line-height-12">
                        <span>{optimalValueHint ?? t("optimalValueHint", { value: optimalValueLabel })}</span>
                        <Button
                            variant="link"
                            size="sm"
                            className="field-with-default-use-optimal p-0 border-0"
                            onClick={() => changeValue(optimalValue)}
                            disabled={formState.isSubmitting}
                            aria-describedby={labelId}
                        >
                            {t("useOptimalValue", { value: optimalValueLabel })}
                        </Button>
                    </div>
                )}
            </div>
        </Form.Group>
    );
}

function splitProps<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
    TInputProps extends object,
>(props: FieldWithDefaultProps<TFieldValues, TName> & TInputProps) {
    return {
        fieldProps: pick(props, fieldWithDefaultPropNames) as FieldWithDefaultProps<TFieldValues, TName>,
        inputProps: omit(props, fieldWithDefaultPropNames) as TInputProps,
    };
}
