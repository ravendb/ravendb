import React from "react";
import { Control, ControllerProps, FieldPath, FieldValues, useController } from "react-hook-form";
import { Input, InputProps, Label } from "reactstrap";
import { InputType } from "reactstrap/types/lib/Input";

type FormElementProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = Omit<
    ControllerProps<TFieldValues, TName>,
    "render" | "control"
> & {
    control: Control<TFieldValues>;
};

// You can specify 'label' without 'labelPosition' or 'label' with 'labelPosition', but not 'labelPosition' without 'label'
type LabelProps =
    | {
          label?: never;
          labelPosition?: never;
      }
    | {
          label: string;
          labelPosition?: "left" | "right";
      };

type FormToggleInputProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = FormElementProps<
    TFieldValues,
    TName
> &
    Omit<InputProps, "type"> & { type: Extract<InputType, "checkbox" | "switch" | "radio"> } & LabelProps &
    AfterChangeProps;

type FormInputProps = InputProps & {
    canBeChecked?: boolean;
};

interface FormSelectOptionProps<T extends string | number = string> {
    value: T;
    label: string;
}
interface AfterChangeProps {
    afterChange?: (event: React.ChangeEvent<HTMLInputElement>) => void;
}

// TODO: simplify template
// TODO: delete checkbox, switch and radio from type input
export function FormInput<
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>
>(props: FormElementProps<TFieldValues, TName> & FormInputProps & AfterChangeProps) {
    const {
        id,
        name,
        control,
        defaultValue,
        rules,
        canBeChecked,
        shouldUnregister,
        children,
        afterChange,
        type,
        ...restInputProps
    } = props;

    const {
        field: { onChange, onBlur, value },
        fieldState: { error, invalid },
    } = useController({
        name,
        control,
        rules,
        defaultValue,
        shouldUnregister,
    });

    return (
        <div>
            <Input
                id={id}
                name={name}
                type={type}
                onBlur={onBlur}
                onChange={(x) => {
                    onChange(x);
                    afterChange?.(x);
                }}
                value={value || ""}
                invalid={invalid}
                checked={canBeChecked && value}
                {...restInputProps}
            >
                {children}
            </Input>
            {/* TODO: error message styling */}
            {error && <div className="text-danger small">{error.message}</div>}
        </div>
    );
}

export function FormGeneralToggle<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormToggleInputProps<TFieldValues, TName>
) {
    const { label, type, ...restProps } = props;
    const labelPosition = props.labelPosition || "right";

    return (
        <Label className="form-check">
            {label && (!labelPosition || labelPosition === "left") && <div className="ms-2">{label}</div>}
            <FormInput type={type} canBeChecked {...restProps} />
            {label && labelPosition === "right" && <div className="ms-2">{label}</div>}
        </Label>
    );
}

export function FormToggle<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormToggleInputProps<TFieldValues, TName>
) {
    const { type, ...restProps } = props;
    return <FormGeneralToggle type={type} {...restProps} />;
}

export function FormSelectOption<T extends string | number = string>({ value, label }: FormSelectOptionProps<T>) {
    return <option value={value}>{label}</option>;
}
