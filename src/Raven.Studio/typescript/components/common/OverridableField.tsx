import { ReactNode, useEffect } from "react";
import {
    Control,
    FieldPath,
    FieldValues,
    get,
    PathValue,
    UseFormSetValue,
    useFormState,
    useWatch,
} from "react-hook-form";
import classNames from "classnames";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import FieldLabel from "components/common/FieldLabel";
import { Icon } from "components/common/Icon";
import { PopoverWithHoverProps } from "components/common/PopoverWithHover";
import useUniqueId from "components/hooks/useUniqueId";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import "./OverridableField.scss";

export interface OverridableFieldRenderState {
    isOverridden: boolean;
    isDisabled: boolean;
    controlId: string;
    labelId: string;
}

interface OverridableFieldProps<
    TFieldValues extends FieldValues,
    TOverrideName extends FieldPath<TFieldValues>,
    TValueName extends FieldPath<TFieldValues>,
> {
    control: Control<TFieldValues>;
    setValue: UseFormSetValue<TFieldValues>;
    overrideName: TOverrideName;
    valueName: TValueName;
    label: ReactNode;
    tooltip?: ReactNode;
    tooltipPlacement?: PopoverWithHoverProps["placement"];
    disabled?: boolean;
    children: (state: OverridableFieldRenderState) => ReactNode;
    optimalValue?: PathValue<TFieldValues, TValueName>;
    optimalValueLabel?: string;
    optimalValueHint?: ReactNode;
}

export default function OverridableField<
    TFieldValues extends FieldValues,
    TOverrideName extends FieldPath<TFieldValues>,
    TValueName extends FieldPath<TFieldValues>,
>({
    control,
    setValue,
    overrideName,
    valueName,
    label,
    tooltip,
    tooltipPlacement,
    disabled,
    children,
    optimalValue,
    optimalValueLabel = String(optimalValue),
    optimalValueHint,
}: OverridableFieldProps<TFieldValues, TOverrideName, TValueName>) {
    const t = useStudioTranslation("overridableField");
    const isOverrideOn = !!useWatch({ control, name: overrideName });
    const value = useWatch({ control, name: valueName });
    const isOverridden = value != null && value !== "";
    const { isSubmitting, errors } = useFormState({ control });
    const hasError = !!get(errors, valueName);

    const controlId = useUniqueId("overridable-field-control-");
    const labelId = useUniqueId("overridable-field-label-");

    useEffect(() => {
        if (isOverrideOn === isOverridden) {
            return;
        }

        setValue(overrideName, isOverridden as PathValue<TFieldValues, TOverrideName>);
        setValue(valueName, isOverridden ? value : (null as PathValue<TFieldValues, TValueName>), {
            shouldValidate: true,
        });
    }, [isOverrideOn, isOverridden, value, overrideName, valueName, setValue]);

    const changeValue = (newValue: PathValue<TFieldValues, TValueName>) => {
        setValue(valueName, newValue, { shouldDirty: true, shouldValidate: true });
    };

    const hasOptimalValue = optimalValue != null;
    const isOptimal = hasOptimalValue && value === optimalValue;
    const isOptimalSuggested = hasOptimalValue && !disabled && !isOptimal && !hasError;

    return (
        <Form.Group controlId={controlId}>
            <div className="overridable-field-label-row">
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
                {isOverridden && !disabled && (
                    <Button
                        variant="link"
                        size="sm"
                        className="p-0 border-0"
                        onClick={() => changeValue(null as PathValue<TFieldValues, TValueName>)}
                        disabled={isSubmitting}
                        aria-describedby={labelId}
                    >
                        <Icon icon="reset" />
                        {t("resetToDefault")}
                    </Button>
                )}
            </div>
            <div className={classNames({ "overridable-field-has-hint": isOptimalSuggested })}>
                {children({ isOverridden, isDisabled: disabled, controlId, labelId })}
                {isOptimalSuggested && (
                    <div className="overridable-field-hint column-gap-1 font-size-10 line-height-12">
                        <span>{optimalValueHint ?? t("optimalValueHint", { value: optimalValueLabel })}</span>
                        <Button
                            variant="link"
                            size="sm"
                            className="overridable-field-use-optimal p-0 border-0"
                            onClick={() => changeValue(optimalValue)}
                            disabled={isSubmitting}
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
