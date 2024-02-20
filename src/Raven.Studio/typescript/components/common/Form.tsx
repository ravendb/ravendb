import React, { ComponentProps, ReactNode, useState } from "react";
import genUtils from "common/generalUtils";
import { Checkbox, CheckboxProps, Radio, Switch } from "components/common/Checkbox";
import { Control, ControllerProps, FieldPath, FieldValues, useController } from "react-hook-form";
import { Button, Input, InputGroup, InputGroupText, InputProps } from "reactstrap";
import { InputType } from "reactstrap/types/lib/Input";
import { RadioToggleWithIcon } from "./RadioToggle";
import AceEditor, { AceEditorProps } from "./AceEditor";
import classNames from "classnames";
import DurationPicker, { DurationPickerProps } from "./DurationPicker";
import SelectCreatable from "./select/SelectCreatable";
import { GetOptionValue, GroupBase, OnChangeValue, OptionsOrGroups } from "react-select";
import Select, { SelectValue } from "./select/Select";
import DatePicker from "./DatePicker";
import { Icon } from "components/common/Icon";

type FormElementProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = Omit<
    ControllerProps<TFieldValues, TName>,
    "render" | "control"
> & {
    control: Control<TFieldValues>;
};

interface AddonProps {
    addon?: ReactNode;
}

type FormInputProps = Omit<InputProps, "addon"> &
    AddonProps & {
        type: InputType;
        passwordPreview?: boolean;
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
    TName extends FieldPath<TFieldValues>,
> = FormElementProps<TFieldValues, TName> &
    Omit<ComponentProps<typeof RadioToggleWithIcon>, "name" | "selectedValue" | "setSelectedValue">;

export function FormInput<
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
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
        formState,
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
        <div className="position-relative flex-grow-1">
            <div className={classNames("d-flex flex-grow-1 flex-vertical", className)}>
                {options.map((option) => (
                    <Checkbox
                        key={option.value}
                        className={checkboxClassName}
                        selected={selectedValues.includes(option.value)}
                        toggleSelection={(x) => toggleSelection(x.currentTarget.checked, option.value)}
                        disabled={formState.isSubmitting}
                    >
                        {option.label}
                    </Checkbox>
                ))}
                {invalid && (
                    <div className="position-absolute badge bg-danger rounded-pill margin-top-xxs">{error.message}</div>
                )}
            </div>
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

export function getFormSelectedOptions<Option>(
    formValues: SelectValue | SelectValue[],
    optionsOrGroups: OptionsOrGroups<Option, GroupBase<Option>>,
    valueAccessor: GetOptionValue<Option>
): Option | GroupBase<Option> | (Option | GroupBase<Option>)[] {
    const optionsFromGroups: Option[] = optionsOrGroups
        .filter((x: GroupBase<Option>) => x.options != null)
        .map((x: GroupBase<Option>) => x.options)
        .flat();

    const basicOptions = optionsOrGroups.filter((x: GroupBase<Option>) => x.options == null) as Option[];

    const allOptions: Option[] = [...optionsFromGroups, ...basicOptions];

    return Array.isArray(formValues)
        ? formValues.map((value) => allOptions.find((option) => _.isEqual(valueAccessor(option), value)))
        : allOptions.find((option) => _.isEqual(valueAccessor(option), formValues));
}

export function FormSelect<
    Option,
    IsMulti extends boolean = false,
    Group extends GroupBase<Option> = GroupBase<Option>,
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
>(props: FormElementProps<TFieldValues, TName> & ComponentProps<typeof Select<Option, IsMulti, Group>>) {
    const { name, control, defaultValue, rules, shouldUnregister, className, ...rest } = props;

    const {
        field: { onChange, value: formValues },
        fieldState: { invalid, error },
        formState,
    } = useController({
        name,
        control,
        rules,
        defaultValue,
        shouldUnregister,
    });

    const valueAccessor = rest.getOptionValue ?? ((option: any) => option.value);

    const selectedOptions = getFormSelectedOptions<Option>(formValues, rest.options, valueAccessor);

    return (
        <div className={classNames("position-relative flex-grow-1", className)}>
            <div className="d-flex flex-grow-1">
                <Select
                    value={selectedOptions}
                    onChange={(options: OnChangeValue<Option, IsMulti>) => {
                        onChange(
                            Array.isArray(options) ? options.map((x) => valueAccessor(x)) : valueAccessor(options)
                        );
                    }}
                    isDisabled={formState.isSubmitting}
                    {...rest}
                />
            </div>
            {invalid && (
                <div className="position-absolute badge bg-danger rounded-pill margin-top-xxs">{error.message}</div>
            )}
        </div>
    );
}

export function FormSelectCreatable<
    Option,
    IsMulti extends boolean = false,
    Group extends GroupBase<Option> = GroupBase<Option>,
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
>(
    props: FormElementProps<TFieldValues, TName> &
        ComponentProps<typeof SelectCreatable<Option, IsMulti, Group>> & {
            customOptions?: OptionsOrGroups<Option, Group>;
            optionCreator?: (value: string) => any;
        }
) {
    const { name, control, defaultValue, rules, shouldUnregister, ...rest } = props;

    const {
        field: { onChange, value: formValues },
        fieldState: { invalid, error },
        formState,
    } = useController({
        name,
        control,
        rules,
        defaultValue,
        shouldUnregister,
    });

    const [customOptions, setCustomOptions] = useState<OptionsOrGroups<Option, Group>>(rest.customOptions ?? []);

    const valueAccessor = rest.getOptionValue ?? ((option: any) => option.value);
    const optionCreator = rest.optionCreator ?? ((value: string) => ({ value, label: value }));

    const selectedOptions = getFormSelectedOptions<Option>(
        formValues,
        [...rest.options, ...customOptions],
        valueAccessor
    );

    const onCreateOption = (value: string) => {
        setCustomOptions((options) => [...options, optionCreator(value)]);
        onChange(rest.isMulti ? [...formValues, value] : value);
    };

    return (
        <div className="position-relative flex-grow-1">
            <div className="d-flex flex-grow-1">
                <SelectCreatable
                    value={selectedOptions}
                    onChange={(options: OnChangeValue<Option, IsMulti>) => {
                        onChange(
                            Array.isArray(options) ? options.map((x) => valueAccessor(x)) : valueAccessor(options)
                        );
                    }}
                    onCreateOption={onCreateOption}
                    disabled={formState.isSubmitting}
                    {...rest}
                />
            </div>
            {invalid && (
                <div className="position-absolute badge bg-danger rounded-pill margin-top-xxs">{error.message}</div>
            )}
        </div>
    );
}

export function FormRadioToggleWithIcon<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormRadioToggleWithIconProps<TFieldValues, TName>
) {
    const { name, control, rules, defaultValue, shouldUnregister, ...rest } = props;

    const {
        field: { onChange, value },
        fieldState: { error, invalid },
        formState,
    } = useController({
        name,
        control,
        rules,
        defaultValue,
        shouldUnregister,
    });

    return (
        <div className="position-relative flex-grow-1">
            <div className="d-flex flex-grow-1">
                <RadioToggleWithIcon
                    name={name}
                    selectedValue={value}
                    setSelectedValue={onChange}
                    disabled={formState.isSubmitting}
                    {...rest}
                />
            </div>
            {invalid && (
                <div className="position-absolute badge bg-danger rounded-pill margin-top-xxs">{error.message}</div>
            )}
        </div>
    );
}

export function FormAceEditor<
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
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

export function FormDurationPicker<
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
>(props: FormElementProps<TFieldValues, TName> & Omit<DurationPickerProps, "onChange" | "totalSeconds">) {
    const { name, control, defaultValue, rules, shouldUnregister, ...rest } = props;

    const {
        field: { onChange, value },
        fieldState: { error },
        formState,
    } = useController({
        name,
        control,
        rules,
        defaultValue,
        shouldUnregister,
    });

    return (
        <div className="position-relative flex-grow-1">
            <div className="d-flex flex-grow-1">
                <DurationPicker totalSeconds={value} onChange={onChange} disabled={formState.isSubmitting} {...rest} />
            </div>
            {error && <div className="position-absolute badge bg-danger rounded-pill">{error.message}</div>}
        </div>
    );
}

export function FormDatePicker<
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
>(props: FormElementProps<TFieldValues, TName> & Omit<ComponentProps<typeof DatePicker>, "onChange"> & AddonProps) {
    const { name, control, defaultValue, rules, shouldUnregister, addon, ...rest } = props;

    const {
        field: { onChange, value },
        fieldState: { error, invalid },
        formState,
    } = useController({
        name,
        control,
        rules,
        defaultValue,
        shouldUnregister,
    });

    return (
        <div className="position-relative flex-grow-1">
            <div className="d-flex flex-grow-1">
                <InputGroup>
                    <DatePicker
                        selected={value}
                        onChange={onChange}
                        invalid={invalid}
                        disabled={formState.isSubmitting}
                        {...rest}
                    />
                    {addon && <InputGroupText>{addon}</InputGroupText>}
                </InputGroup>
            </div>
            {error && (
                <div className="position-absolute badge bg-danger rounded-pill margin-top-xxs">{error.message}</div>
            )}
        </div>
    );
}

function FormInputGeneral<
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
>(props: FormElementProps<TFieldValues, TName> & Omit<InputProps, "addon"> & AddonProps) {
    const { name, control, defaultValue, rules, shouldUnregister, children, type, addon, passwordPreview, ...rest } =
        props;

    const {
        field: { onChange, onBlur, value },
        fieldState: { error, invalid },
        formState,
    } = useController({
        name,
        control,
        rules,
        defaultValue,
        shouldUnregister,
    });

    const handleValueChange = (value: string) => {
        if (type === "number") {
            onChange(value === "" ? null : Number(value));
        } else {
            onChange(value);
        }
    };

    const [showPassword, setShowPassword] = useState(false);

    const actualInputType = showPassword ? "text" : type;

    return (
        <div className="position-relative flex-grow-1">
            <div className="d-flex flex-grow-1">
                <InputGroup>
                    <Input
                        name={name}
                        type={actualInputType}
                        onBlur={onBlur}
                        onChange={(x) => handleValueChange(x.currentTarget.value)}
                        value={value == null ? "" : value}
                        invalid={invalid}
                        className={classNames(
                            "position-relative d-flex flex-grow-1",
                            passwordPreview ? "preview-password" : null
                        )}
                        disabled={formState.isSubmitting}
                        {...rest}
                    >
                        {children}
                    </Input>
                    {addon && <InputGroupText>{addon}</InputGroupText>}
                    {passwordPreview && (
                        <Button
                            color="link-muted"
                            onClick={() => setShowPassword(!showPassword)}
                            className={classNames("btn-preview position-absolute end-0 h-100", invalid && "me-3")}
                        >
                            {showPassword ? (
                                <Icon icon="preview-off" title="Hide password" margin="m-0" />
                            ) : (
                                <Icon icon="preview" title="Show password" margin="m-0" />
                            )}
                        </Button>
                    )}
                </InputGroup>
            </div>

            {error && (
                <div className="position-absolute badge bg-danger rounded-pill margin-top-xxs">{error.message}</div>
            )}
        </div>
    );
}

function FormToggle<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormToggleProps<TFieldValues, TName> & { type: Extract<InputType, "checkbox" | "switch" | "radio"> }
) {
    const { name, control, rules, defaultValue, type, shouldUnregister, ...rest } = props;

    const {
        field: { onChange, onBlur, value },
        fieldState: { error, invalid },
        formState,
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
        <div className="position-relative flex-grow-1">
            <div className="d-flex flex-grow-1">
                <ToggleComponent
                    selected={!!value}
                    toggleSelection={onChange}
                    invalid={invalid}
                    onBlur={onBlur}
                    color="primary"
                    disabled={formState.isSubmitting}
                    {...rest}
                />
            </div>
            {invalid && <div className="position-absolute badge bg-danger rounded-pill">{error.message}</div>}
        </div>
    );
}
