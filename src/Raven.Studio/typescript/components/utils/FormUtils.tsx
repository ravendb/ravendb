import React from "react";
import { Control, Controller, ControllerProps, FieldPath, FieldValues } from "react-hook-form";
import { Input, InputProps, Label } from "reactstrap";

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

interface FormSelectOptionProps<T extends string | number = string> {
    value: T;
    label: string;
}

// TODO: simplify template
// TODO: delete type checkbox from type input
export function FormInput<
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>
>(props: FormElementProps<TFieldValues, TName> & InputProps) {
    const { control, name, defaultValue, rules, shouldUnregister, children, type, ...restInputProps } = props;

    const canBeChecked = type === "checkbox" || type === "switch";

    return (
        <Controller
            name={name}
            control={control}
            rules={rules}
            defaultValue={defaultValue}
            shouldUnregister={shouldUnregister}
            render={({ field: { onChange, onBlur, value }, fieldState: { error, invalid } }) => (
                <div>
                    <Input
                        onBlur={onBlur}
                        onChange={onChange}
                        value={value === undefined ? "" : value}
                        invalid={invalid}
                        type={type}
                        checked={canBeChecked && value}
                        {...restInputProps}
                    >
                        {children}
                    </Input>
                    {/* TODO: error message styling */}
                    {error && <div className="text-danger small">{error.message}</div>}
                </div>
            )}
        />
    );
}

export function FormGeneralToggle<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormElementProps<TFieldValues, TName> &
        Omit<InputProps, "type"> & { type: "checkbox" | "switch" } & LabelProps
) {
    const { label, type, ...restProps } = props;
    const labelPosition = props.labelPosition || "right";

    return (
        <Label className="form-check">
            {label && labelPosition === "left" && <div className="ms-2">{label}</div>}
            <FormInput type={type} {...restProps} />
            {label && labelPosition === "right" && <div className="ms-2">{label}</div>}
        </Label>
    );
}

export function FormCheckbox<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormElementProps<TFieldValues, TName> & Omit<InputProps, "type"> & LabelProps
) {
    return <FormGeneralToggle type="checkbox" {...props} />;
}

export function FormSwitch<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormElementProps<TFieldValues, TName> & Omit<InputProps, "type"> & LabelProps
) {
    return <FormGeneralToggle type="switch" {...props} />;
}

export function FormSelectOption<T extends string | number = string>({ value, label }: FormSelectOptionProps<T>) {
    return <option value={value}>{label}</option>;
}
