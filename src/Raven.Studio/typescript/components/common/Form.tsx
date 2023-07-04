import React from "react";
import genUtils from "common/generalUtils";
import { Checkbox, CheckboxProps, Radio, Switch } from "components/common/Checkbox";
import { Control, ControllerProps, FieldPath, FieldValues, useController } from "react-hook-form";
import { Input, InputProps } from "reactstrap";
import { InputType } from "reactstrap/types/lib/Input";
import { RadioToggleWithIcon, RadioToggleWithIconInputItem } from "./RadioToggle";
import AceEditor, { AceEditorProps } from "./AceEditor";
import Select, { SelectProps } from "./Select";
import classNames from "classnames";

type FormElementProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = Omit<
    ControllerProps<TFieldValues, TName>,
    "render" | "control"
> & {
    control: Control<TFieldValues>;
};

type FormInputProps = InputProps & {
    type: Extract<InputType, "text" | "textarea" | "number" | "password" | "checkbox">;
};

export interface FormCheckboxesOption<T extends string | number = string> {
    value: T;
    label: string;
}

interface FormCheckboxesProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>
    extends FormElementProps<TFieldValues, TName> {
    options: FormCheckboxesOption<TFieldValues[TName][any]>[];
    className?: string;
    checkboxClassName?: string;
}

type FormToggleProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = FormElementProps<
    TFieldValues,
    TName
> &
    Omit<CheckboxProps, "selected" | "toggleSelection">;

type FormRadioToggleWithIconProps<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>
> = FormElementProps<TFieldValues, TName> & {
    leftItem: RadioToggleWithIconInputItem<TFieldValues[TName]>;
    rightItem: RadioToggleWithIconInputItem<TFieldValues[TName]>;
    className?: string;
};

export function FormInput<
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>
>(props: FormElementProps<TFieldValues, TName> & FormInputProps) {
    return <FormInputGeneral {...props} />;
}

export function FormCheckboxes<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormCheckboxesProps<TFieldValues, TName>
) {
    const { name, control, defaultValue, rules, shouldUnregister, options, className, checkboxClassName } = props;

    const {
        field: { onChange, value: selectedValues },
        fieldState: { invalid, error },
    } = useController({
        name,
        control,
        rules,
        defaultValue,
        shouldUnregister,
    });

    const toggleSelection = (isChecked: boolean, optionValue: TFieldValues[TName][any]) => {
        if (isChecked) {
            onChange([...selectedValues, optionValue]);
        } else {
            onChange(selectedValues.filter((x: TFieldValues[TName][any]) => x !== optionValue));
        }
    };

    return (
        <div className={classNames("flex-vertical", className)}>
            {options.map((option) => (
                <Checkbox
                    key={option.value}
                    className={checkboxClassName}
                    selected={selectedValues.includes(option.value)}
                    toggleSelection={(x) => toggleSelection(x.currentTarget.checked, option.value)}
                >
                    {option.label}
                </Checkbox>
            ))}
            {invalid && <div className="text-danger small">{error.message}</div>}
        </div>
    );
}

export function FormCheckbox<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormToggleProps<TFieldValues, TName>
) {
    return <FormToggle type="checkbox" {...props} />;
}

export function FormSwitch<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormToggleProps<TFieldValues, TName>
) {
    return <FormCheckbox type="switch" {...props} />;
}

export function FormRadio<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormToggleProps<TFieldValues, TName>
) {
    return <FormCheckbox type="radio" {...props} />;
}

export function FormSelect<
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>
>(props: FormElementProps<TFieldValues, TName> & Omit<SelectProps<TFieldValues[TName]>, "setSelectedValue">) {
    const { name, control, defaultValue, rules, shouldUnregister, ...rest } = props;

    const {
        field: { onChange, value },
        fieldState: { invalid, error },
    } = useController({
        name,
        control,
        rules,
        defaultValue,
        shouldUnregister,
    });

    return (
        <div>
            <div className="form-dropdown-select">
                <Select setSelectedValue={onChange} outline selectedValue={value} {...rest} />
            </div>
            {invalid && <div className="text-danger small">{error.message}</div>}
        </div>
    );
}

export function FormRadioToggleWithIcon<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormRadioToggleWithIconProps<TFieldValues, TName>
) {
    const { name, control, rules, defaultValue, shouldUnregister, leftItem, rightItem, className } = props;

    const {
        field: { onChange, value },
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
            <RadioToggleWithIcon
                name={name}
                leftItem={leftItem}
                rightItem={rightItem}
                selectedValue={value}
                setSelectedValue={onChange}
                className={className}
            />
            {invalid && <div className="text-danger small">{error.message}</div>}
        </div>
    );
}

export function FormAceEditor<
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>
>(props: FormElementProps<TFieldValues, TName> & AceEditorProps) {
    const { name, control, defaultValue, rules, shouldUnregister, ...rest } = props;

    const {
        field: { onChange, value },
        fieldState: { error },
    } = useController({
        name,
        control,
        rules,
        defaultValue,
        shouldUnregister,
    });

    return <AceEditor onChange={onChange} value={value} validationErrorMessage={error?.message} {...rest} />;
}

function FormInputGeneral<
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>
>(props: FormElementProps<TFieldValues, TName> & InputProps) {
    const { name, control, defaultValue, rules, shouldUnregister, children, type, ...rest } = props;

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
        <>
            <div className="d-flex flex-grow">
                <Input
                    name={name}
                    type={type}
                    onBlur={onBlur}
                    onChange={(x) =>
                        onChange(type === "number" ? Number(x.currentTarget.value) : x.currentTarget.value)
                    }
                    value={value == null ? "" : value}
                    invalid={invalid}
                    {...rest}
                >
                    {children}
                </Input>
            </div>

            {error && <div className="d-flex justify-content-end text-danger small w-100">{error.message}</div>}
        </>
    );
}

function FormToggle<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormToggleProps<TFieldValues, TName> & { type: Extract<InputType, "checkbox" | "switch" | "radio"> }
) {
    const { name, control, rules, defaultValue, type, shouldUnregister, ...rest } = props;

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

    let ToggleComponent: (props: CheckboxProps) => JSX.Element;
    switch (type) {
        case "checkbox":
            ToggleComponent = Checkbox;
            break;
        case "switch":
            ToggleComponent = Switch;
            break;
        case "radio":
            ToggleComponent = Radio;
            break;
        default:
            genUtils.assertUnreachable(type);
    }

    return (
        <div>
            <ToggleComponent
                selected={!!value}
                toggleSelection={onChange}
                invalid={invalid}
                onBlur={onBlur}
                {...rest}
            />
            {invalid && <div className="text-danger small">{error.message}</div>}
        </div>
    );
}
