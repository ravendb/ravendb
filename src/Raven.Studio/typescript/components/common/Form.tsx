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
import { GetOptionValue, GroupBase, InputActionMeta, OnChangeValue, OptionsOrGroups } from "react-select";
import Select, { InputNotHidden, SelectValue } from "./select/Select";
import DatePicker from "./DatePicker";
import { Icon } from "components/common/Icon";
import PathSelector, { PathSelectorProps } from "components/common/pathSelector/PathSelector";
import { OmitIndexSignature } from "components/utils/common";

type FormElementProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = Omit<
    ControllerProps<TFieldValues, TName>,
    "render" | "control"
> & {
    control: Control<TFieldValues>;
};

interface AddonProps {
    addon?: ReactNode;
}

type FormInputProps = Omit<OmitIndexSignature<InputProps>, "addon"> &
    AddonProps & {
        type: InputType;
        passwordPreview?: boolean;
        rows?: number | string;
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
                {invalid && <FormValidationMessage>{error.message}</FormValidationMessage>}
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
        ? formValues.map((value) => allOptions.find((option) => valueAccessor(option) === value))
        : allOptions.find((option) => valueAccessor(option) === formValues);
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
        <>
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
            </div>
            {invalid && <FormValidationMessage>{error.message}</FormValidationMessage>}
        </>
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
            {invalid && <FormValidationMessage>{error.message}</FormValidationMessage>}
        </div>
    );
}

export function FormSelectAutocomplete<
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
    const {
        field: { onChange, value },
    } = useController({
        name: props.name,
    });

    const onInputChange = (value: string, action: InputActionMeta) => {
        if (action?.action !== "input-blur" && action?.action !== "menu-close") {
            onChange(value);
        }
    };

    return (
        <FormSelectCreatable<Option, IsMulti, Group, TFieldValues, TName>
            inputValue={value}
            onInputChange={onInputChange}
            components={{ Input: InputNotHidden }}
            tabSelectsValue
            controlShouldRenderValue={false}
            closeMenuOnSelect={false}
            {...props}
        />
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
            {invalid && <FormValidationMessage>{error.message}</FormValidationMessage>}
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
            {error && <FormValidationMessage>{error.message}</FormValidationMessage>}
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
            {error && <FormValidationMessage>{error.message}</FormValidationMessage>}
        </div>
    );
}

function FormInputGeneral<
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
>(props: FormElementProps<TFieldValues, TName> & FormInputProps) {
    const { name, control, defaultValue, rules, shouldUnregister, children, type, addon, passwordPreview, ...rest } =
        props;

    const {
        field: { onChange, onBlur, value, ref },
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
        <>
            <div className="position-relative flex-grow-1">
                <div className="d-flex flex-grow-1">
                    <InputGroup>
                        <Input
                            innerRef={ref}
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
                                className={classNames("input-btn", invalid && "me-3")}
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
            </div>
            {error && <FormValidationMessage>{error.message}</FormValidationMessage>}
        </>
    );
}

function FormToggle<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormToggleProps<TFieldValues, TName> & { type: Extract<InputType, "checkbox" | "switch" | "radio"> }
) {
    const { name, control, rules, defaultValue, type, shouldUnregister, ...rest } = props;

    const {
        field: { onChange, onBlur, value },
        fieldState: { invalid },
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
        </div>
    );
}

export function FormPathSelector<
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
    ParamsType extends unknown[] = unknown[],
>(props: FormElementProps<TFieldValues, TName> & Omit<PathSelectorProps<ParamsType>, "handleSelect">) {
    const {
        name,
        control,
        defaultValue,
        rules,
        shouldUnregister,
        selectorTitle,
        placeholder,
        getPaths,
        getPathDependencies,
        disabled,
    } = props;

    const {
        field: { onChange, value: formValuePath },
        formState,
        fieldState: { invalid, error },
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
                    <Input
                        name={name}
                        type="text"
                        onChange={(x) => onChange(x.currentTarget.value)}
                        value={formValuePath == null ? "" : formValuePath}
                        invalid={invalid}
                        className="position-relative d-flex flex-grow-1"
                        placeholder={placeholder || "Enter path"}
                        disabled={disabled || formState.isSubmitting}
                    />
                    <PathSelector
                        getPaths={getPaths}
                        getPathDependencies={getPathDependencies}
                        handleSelect={onChange}
                        defaultPath={formValuePath}
                        selectorTitle={selectorTitle}
                        disabled={disabled || formState.isSubmitting}
                        buttonClassName={classNames("input-btn", invalid && "me-3")}
                    />
                </InputGroup>
            </div>
            {error && <FormValidationMessage>{error.message}</FormValidationMessage>}
        </div>
    );
}

function FormValidationMessage(props: { children: string }) {
    const { children } = props;
    return (
        <div className="validation-message text-start w-100 ">
            <div className="badge bg-danger rounded-pill">{children}</div>
        </div>
    );
}
