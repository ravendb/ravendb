import React, { ComponentProps, ReactNode, useCallback, useEffect, useRef, useState } from "react";
import genUtils from "common/generalUtils";
import { Checkbox, CheckboxProps, Radio, Switch } from "components/common/Checkbox";
import {
    Control,
    ControllerProps,
    FieldError,
    FieldPath,
    FieldValues,
    PathValue,
    useController,
    useFormState,
} from "react-hook-form";
import InputGroup from "react-bootstrap/InputGroup";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import { RadioToggleWithIcon } from "./toggles/RadioToggle";
import AceEditor, { AceEditorProps } from "./ace/AceEditor";
import classNames from "classnames";
import DurationPicker, { DurationPickerProps } from "./DurationPicker";
import SelectCreatable from "./select/SelectCreatable";
import { GetOptionValue, GroupBase, InputActionMeta, OnChangeValue, OptionsOrGroups } from "react-select";
import Select, { InputNotHidden, SelectValue } from "./select/Select";
import DatePicker, { type DatePickerProps } from "./DatePicker";
import { Icon } from "components/common/Icon";
import PathSelector, { PathSelectorProps, PathSelectorStateRef } from "components/common/pathSelector/PathSelector";
import { OmitIndexSignature } from "components/utils/common";
import { RavenFormControlProps } from "react-bootstrap/FormControl";
import { FormRangeProps } from "react-bootstrap/FormRange";
import { InputType } from "../../../typings/_studio/react-bootstrap";
import useUniqueId from "hooks/useUniqueId";
import { FormGroupProps as ReactBootstrapFormGroupsProps } from "react-bootstrap/FormGroup";
import useBoolean from "components/hooks/useBoolean";
import { FilterOptionOption } from "react-select/dist/declarations/src/filters";
import { MultiRadioToggle } from "./toggles/MultiRadioToggle";
import "./VerificationCodeInput.scss";
import { ConditionalPopover } from "./ConditionalPopover";

type FormElementProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = Omit<
    ControllerProps<TFieldValues, TName>,
    "render" | "control"
> & {
    control: Control<TFieldValues>;
};

interface AddonProps {
    addon?: ReactNode;
}

type FormInputProps = Omit<OmitIndexSignature<RavenFormControlProps>, "addon"> &
    AddonProps & {
        type: InputType;
        passwordPreview?: boolean;
        rows?: number | string;
    };

export interface FormCheckboxesOption<T extends string | number = string> {
    value: T;
    label: string;
    disabledReason?: string;
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
    Omit<CheckboxProps, "selected" | "toggleSelection"> & { afterChange?: (value: boolean) => void };

type FormRadioToggleWithIconProps<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
> = FormElementProps<TFieldValues, TName> &
    Omit<ComponentProps<typeof RadioToggleWithIcon>, "name" | "selectedValue" | "setSelectedValue">;

type FormMultiRadioToggleProps<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
> = FormElementProps<TFieldValues, TName> &
    Omit<ComponentProps<typeof MultiRadioToggle>, "selectedItem" | "setSelectedItem">;

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
                    <ConditionalPopover
                        key={option.value}
                        conditions={{
                            isActive: Boolean(option.disabledReason),
                            message: option.disabledReason,
                        }}
                    >
                        <Checkbox
                            key={option.value}
                            className={checkboxClassName}
                            selected={selectedValues.includes(option.value)}
                            toggleSelection={(x) => toggleSelection(x.currentTarget.checked, option.value)}
                            disabled={formState.isSubmitting || Boolean(option.disabledReason)}
                        >
                            {option.label}
                        </Checkbox>
                    </ConditionalPopover>
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
    props: FormToggleProps<TFieldValues, TName> & { value: PathValue<TFieldValues, TName> }
) {
    const { name, control, rules, defaultValue, shouldUnregister, ...rest } = props;

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

    return (
        <div className="position-relative">
            <div className="d-flex flex-grow-1">
                <Radio
                    selected={value === props.value}
                    toggleSelection={() => {
                        onChange(props.value);
                    }}
                    isInvalid={invalid}
                    onBlur={onBlur}
                    color="primary"
                    disabled={formState.isSubmitting}
                    {...rest}
                />
            </div>
        </div>
    );
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
        ? formValues.map((value) =>
              allOptions.find((option) => JSON.stringify(valueAccessor(option)) === JSON.stringify(value))
          )
        : allOptions.find((option) => JSON.stringify(valueAccessor(option)) === JSON.stringify(formValues));
}

export function FormSelect<
    Option,
    IsMulti extends boolean = false,
    Group extends GroupBase<Option> = GroupBase<Option>,
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
>(
    props: FormElementProps<TFieldValues, TName> &
        ComponentProps<typeof Select<Option, IsMulti, Group>> & { selectClassName?: string }
) {
    const { name, control, defaultValue, rules, shouldUnregister, className, selectClassName, ...rest } = props;

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

    const selectedOptions = getFormSelectedOptions<Option>(formValues, rest.options, valueAccessor) ?? null;

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
                        className={classNames(selectClassName, invalid ? "is-invalid" : "")}
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
            addon?: ReactNode | string;
        }
) {
    const { name, control, defaultValue, rules, shouldUnregister, addon, ...rest } = props;

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
    const optionCreator = rest.optionCreator ?? ((value: string) => ({ value, label: value }));

    const getOptionsFromValue = (
        formValues: PathValue<TFieldValues, TName>,
        creator: (value: string) => Option
    ): Option[] => {
        if (!formValues) {
            return [];
        }
        return Array.isArray(formValues) ? formValues.map(creator) : [creator(formValues)];
    };

    const [customOptions, setCustomOptions] = useState<OptionsOrGroups<Option, Group>>(
        rest.customOptions ?? getOptionsFromValue(formValues, optionCreator)
    );

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
            <InputGroup className="d-flex flex-grow-1">
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
                {addon && <InputGroup.Text>{addon}</InputGroup.Text>}
            </InputGroup>
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
            addon?: ReactNode | string;
        }
) {
    const {
        field: { onChange, value },
    } = useController({
        name: props.name,
        control: props.control,
    });

    const { value: isInitialOpen, setValue: setIsInitialOpen } = useBoolean(false);

    const valueAccessor = props.getOptionValue ?? ((option: any) => option.value);
    const labelAccessor = props.getOptionLabel ?? ((option: any) => option.label);

    const handleFilterOption = (option: FilterOptionOption<Option>, inputValue: string) => {
        if (isInitialOpen) {
            return true;
        }

        return (
            compareInputValue(valueAccessor(option), inputValue) || compareInputValue(labelAccessor(option), inputValue)
        );
    };

    const compareInputValue = (value: unknown, inputValue: string): boolean => {
        return String(value).trim().toLowerCase().includes(String(inputValue).trim().toLowerCase());
    };

    const handleInputChange = (value: string, action: InputActionMeta) => {
        if (action?.action === "input-change") {
            onChange(value);
            setIsInitialOpen(false);
            return;
        }
        if (action?.action === "set-value") {
            // Prevent clearing the input when an option is selected/created.
            return;
        }
    };

    const handleFocus = (e: React.FocusEvent<HTMLInputElement, Element>) => {
        e.target.selectionStart = String(value).length;
        setIsInitialOpen(true);
    };

    const inputValue = props.isDisabled ? "" : (value ?? "");
    const components = props.components ? { ...props.components, Input: InputNotHidden } : { Input: InputNotHidden };

    return (
        <FormSelectCreatable<Option, IsMulti, Group, TFieldValues, TName>
            inputValue={inputValue}
            onInputChange={handleInputChange}
            tabSelectsValue
            controlShouldRenderValue={!!props.isDisabled}
            filterOption={handleFilterOption}
            onFocus={handleFocus}
            blurInputOnSelect
            {...props}
            components={components} // Override to ensure Input is not hidden
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

export function FormMultiRadioToggle<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormMultiRadioToggleProps<TFieldValues, TName>
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
                <MultiRadioToggle
                    selectedItem={value}
                    setSelectedItem={(x) => onChange(x)}
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
    const { name, control, defaultValue, rules, shouldUnregister, disabled, ...rest } = props;

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
        <AceEditor
            onChange={onChange}
            value={value}
            validationErrorMessage={error?.message}
            disabled={formState.isSubmitting || disabled}
            {...rest}
        />
    );
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
>(props: FormElementProps<TFieldValues, TName> & DatePickerProps & AddonProps) {
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

    const datePickerProps: DatePickerProps = {
        ...rest,
        selected: value,
        onChange,
        isInvalid: invalid,
        disabled: formState.isSubmitting || rest.disabled,
    };

    return (
        <div className="position-relative flex-grow-1 z-2">
            <div className="d-flex flex-grow-1">
                <InputGroup>
                    <DatePicker {...datePickerProps} />
                    {addon && <InputGroup.Text>{addon}</InputGroup.Text>}
                </InputGroup>
            </div>
            {error && <FormValidationMessage>{error.message}</FormValidationMessage>}
        </div>
    );
}

function FormInputGeneral<
    TFieldValues extends FieldValues = FieldValues,
    TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
>(props: FormElementProps<TFieldValues, TName> & Omit<FormInputProps, "size">) {
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
                        <Form.Control
                            ref={ref}
                            name={name}
                            type={actualInputType}
                            onBlur={onBlur}
                            onChange={(x) => handleValueChange(x.currentTarget.value)}
                            value={value == null ? "" : value}
                            isInvalid={invalid}
                            className={classNames(
                                "position-relative d-flex flex-grow-1",
                                passwordPreview ? "preview-password" : null
                            )}
                            disabled={formState.isSubmitting}
                            {...rest}
                        >
                            {children}
                        </Form.Control>
                        {addon && <InputGroup.Text>{addon}</InputGroup.Text>}
                        {passwordPreview && (
                            <Button
                                variant="link-muted"
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
    const { name, control, rules, defaultValue, type, shouldUnregister, afterChange, ...rest } = props;

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

    let ToggleComponent: (props: CheckboxProps) => React.JSX.Element;
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
        <div className="position-relative">
            <div className="d-flex flex-grow-1">
                <ToggleComponent
                    selected={!!value}
                    toggleSelection={(x) => {
                        onChange(x);
                        afterChange?.(x.currentTarget.checked);
                    }}
                    isInvalid={invalid}
                    onBlur={onBlur}
                    color="primary"
                    disabled={formState.isSubmitting}
                    {...rest}
                />
            </div>
        </div>
    );
}

export function FormRange<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormElementProps<TFieldValues, TName> & FormRangeProps
) {
    const { name, control, rules, defaultValue, shouldUnregister, ...rest } = props;

    const { field, formState } = useController({
        name,
        control,
        rules,
        defaultValue,
        shouldUnregister,
    });

    return (
        <div className="position-relative">
            <div className="d-flex flex-grow-1">
                <Form.Range disabled={formState.isSubmitting || field.disabled} {...field} {...rest} />
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
        getPathsProvider,
        getPathDependencies,
        disabled,
        ...rest
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

    const pathSelectorStateRef = useRef<PathSelectorStateRef>(null);
    const inputRef = useRef<HTMLInputElement>(null);

    const handleInputFocus = () => {
        if (!formValuePath) {
            inputRef.current.blur();
            pathSelectorStateRef.current.toggle();
        }
    };

    return (
        <div className="position-relative flex-grow-1">
            <div className="d-flex flex-grow-1">
                <InputGroup>
                    <Form.Control
                        ref={inputRef}
                        name={name}
                        type="text"
                        onChange={(x) => onChange(x.currentTarget.value)}
                        value={formValuePath == null ? "" : formValuePath}
                        isInvalid={invalid}
                        className="position-relative d-flex flex-grow-1"
                        placeholder={placeholder || "Enter path"}
                        disabled={disabled || formState.isSubmitting}
                        onFocus={handleInputFocus}
                    />
                    <PathSelector
                        getPathsProvider={getPathsProvider}
                        getPathDependencies={getPathDependencies}
                        handleSelect={onChange}
                        defaultPath={formValuePath}
                        selectorTitle={selectorTitle}
                        disabled={disabled || formState.isSubmitting}
                        buttonClassName={classNames("input-btn", invalid && "me-3")}
                        stateRef={pathSelectorStateRef}
                        {...rest}
                    />
                </InputGroup>
            </div>
            {error && <FormValidationMessage>{error.message}</FormValidationMessage>}
        </div>
    );
}

export function FormValidationMessage(props: { children: string; className?: string }) {
    const { children, className } = props;
    return (
        <div className={classNames("validation-message text-start w-100", className)}>
            <div className="badge bg-danger rounded-pill">{children}</div>
        </div>
    );
}

interface FormGroupProps extends ReactBootstrapFormGroupsProps {
    marginClass?: string;
}

export function FormGroup({ marginClass = "mb-3", ...props }: FormGroupProps) {
    const uniqueId = useUniqueId("form-group-");

    return (
        <Form.Group {...props} className={classNames(props.className, marginClass)} controlId={uniqueId}>
            {props.children}
        </Form.Group>
    );
}

export const FormLabel = Form.Label;

export function OptionalLabel() {
    return <small className="text-muted fw-light">(optional)</small>;
}

interface VerificationCodeInputProps {
    name: string;
    control: Control;
    onLastDigitInsertSubmit?: (code: string) => void;
}

export const FormVerificationCodeInput = ({ name, control, onLastDigitInsertSubmit }: VerificationCodeInputProps) => {
    const {
        field: { onChange, ref },
        fieldState: { error },
    } = useController({
        name,
        control,
    });

    const [code, setCode] = useState<string[]>(Array(6).fill(""));
    const inputRefs = useRef<HTMLInputElement[]>(Array(6).fill(null));

    const firstInputRef = useCallback(
        (input: HTMLInputElement | null) => {
            inputRefs.current[0] = input;
            if (ref) {
                ref(input);
            }
        },
        [ref]
    );

    const handleChange = (e: React.ChangeEvent<HTMLInputElement>, index: number) => {
        const { value } = e.target;

        if (!/^\d$/.test(value) && value !== "") {
            return;
        }

        const newCode = [...code];
        newCode[index] = value;
        setCode(newCode);

        onChange(newCode.join(""));

        if (value && index < 5) {
            inputRefs.current[index + 1]?.focus();
        }

        // Submit when the last digit is entered and onLastDigitInsertSubmit is provided
        if (value && index === 5 && onLastDigitInsertSubmit) {
            onLastDigitInsertSubmit(newCode.join(""));
        }
    };

    const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>, index: number) => {
        if (e.key === "Backspace" && !code[index] && index > 0) {
            inputRefs.current[index - 1]?.focus();
        }
    };

    const handleFocus = (e: React.FocusEvent<HTMLInputElement>) => {
        e.target.select();
    };

    const handlePaste = (e: React.ClipboardEvent<HTMLInputElement>) => {
        e.preventDefault();
        const pastedData = e.clipboardData.getData("text").replace(/\D/g, "").slice(0, 6);
        const newCode = [...code];

        for (let i = 0; i < pastedData.length; i++) {
            newCode[i] = pastedData[i];
        }

        setCode(newCode);
        onChange(newCode.join(""));

        if (pastedData.length === 6 && onLastDigitInsertSubmit) {
            onLastDigitInsertSubmit(newCode.join(""));
        }

        if (pastedData.length < 6) {
            inputRefs.current[pastedData.length]?.focus();
        }
    };

    return (
        <div>
            <div className="verification-code-inputs">
                {code.map((digit, index) => (
                    <Form.Control
                        key={index}
                        type="text"
                        maxLength={1}
                        value={digit}
                        onChange={(e: React.ChangeEvent<HTMLInputElement>) => handleChange(e, index)}
                        onKeyDown={(e: React.KeyboardEvent<HTMLInputElement>) => handleKeyDown(e, index)}
                        onFocus={handleFocus}
                        onPaste={handlePaste}
                        ref={
                            index === 0
                                ? firstInputRef
                                : (input: HTMLInputElement | null) => {
                                      inputRefs.current[index] = input;
                                  }
                        }
                        autoComplete="off"
                        className="text-center"
                        isInvalid={!!error}
                    />
                ))}
            </div>
            {error && <FormValidationMessage className="mt-1">{error.message}</FormValidationMessage>}
        </div>
    );
};

interface FormErrorIconProps<TFieldValues extends FieldValues> {
    control: Control<TFieldValues>;
    paths: FieldPath<TFieldValues>[];
    onError?: () => void;
    errorMessage?: ReactNode;
}

export function FormErrorIcon<TFieldValues extends FieldValues>({
    control,
    paths,
    onError,
}: FormErrorIconProps<TFieldValues>) {
    const { hasErrors, message } = useErrorMessage({ control, paths });

    useEffect(() => {
        if (hasErrors) {
            onError?.();
        }
    }, [hasErrors]);

    if (!hasErrors) {
        return null;
    }

    return (
        <ConditionalPopover
            conditions={{
                isActive: hasErrors,
                message: message,
            }}
        >
            <Icon icon="warning" color="danger" margin="ms-1" />
        </ConditionalPopover>
    );
}

interface UseErrorMessageProps<TFieldValues extends FieldValues> {
    control: Control<TFieldValues>;
    paths: FieldPath<TFieldValues>[];
}

export function useErrorMessage<TFieldValues extends FieldValues>({
    control,
    paths,
}: UseErrorMessageProps<TFieldValues>) {
    const formState = useFormState({
        control,
        name: paths,
    });

    let error: FieldError = undefined;

    for (const path of paths) {
        const fieldError = _.get(formState.errors, path);

        // For array fields, react-hook-form nests the error under "root"
        error = fieldError?.message ? fieldError : fieldError?.root;

        if (error) {
            break;
        }
    }

    return {
        hasErrors: !!error,
        message: error?.message,
    };
}

export function hasRelevantDirtyFields(dirtyFields: unknown, ignoredFieldNames: string[]): boolean {
    if (!dirtyFields) {
        return false;
    }

    if (dirtyFields === true) {
        return true;
    }

    if (Array.isArray(dirtyFields)) {
        return dirtyFields.some((field) => hasRelevantDirtyFields(field, ignoredFieldNames));
    }

    if (typeof dirtyFields === "object") {
        return Object.entries(dirtyFields).some(([key, value]) => {
            if (ignoredFieldNames.includes(key)) {
                return false;
            }

            return hasRelevantDirtyFields(value, ignoredFieldNames);
        });
    }

    return false;
}
