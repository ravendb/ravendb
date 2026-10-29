import { Meta } from "@storybook/react-webpack5";
import { useEffect } from "react";
import { useForm, useWatch } from "react-hook-form";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { FormDurationPicker, FormInput, FormSelect } from "components/common/Form";
import OverridableField from "components/common/OverridableField";
import { SelectOption } from "components/common/select/Select";

export default {
    title: "Bits/OverridableField",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

const schema = yup.object({
    isDefaultOverridden: yup.boolean(),
    defaultValue: yup.number().nullable(),
    isOverriddenOverridden: yup.boolean(),
    overriddenValue: yup.number().nullable(),
    isDisabledOverridden: yup.boolean(),
    disabledValue: yup.number().nullable(),
    isSelectOverridden: yup.boolean(),
    selectValue: yup.string().nullable(),
    isAddonOverridden: yup.boolean(),
    addonValue: yup.number().nullable(),
    isInvalidOverridden: yup.boolean(),
    invalidValue: yup.number().nullable().min(1, "Value must be at least 1"),
    isNarrowOverridden: yup.boolean(),
    narrowValue: yup.number().nullable(),
    isDurationOverridden: yup.boolean(),
    durationValue: yup.number().nullable(),
    isOptimalOverridden: yup.boolean(),
    optimalItems: yup.number().nullable().min(1, "Enter at least 1 item"),
});

type FormData = yup.InferType<typeof schema>;

const defaultValues: FormData = {
    isDefaultOverridden: false,
    defaultValue: null,
    isOverriddenOverridden: true,
    overriddenValue: 45,
    isDisabledOverridden: true,
    disabledValue: 30,
    isSelectOverridden: false,
    selectValue: null,
    isAddonOverridden: true,
    addonValue: 60,
    isInvalidOverridden: true,
    invalidValue: 0,
    isNarrowOverridden: false,
    narrowValue: null,
    isDurationOverridden: true,
    durationValue: 3600,
    isOptimalOverridden: false,
    optimalItems: null,
};

const selectOptions: SelectOption[] = [
    { label: "None", value: "None" },
    { label: "Round Robin", value: "RoundRobin" },
    { label: "Fastest Node", value: "FastestNode" },
];

export function States() {
    const { control, trigger, setValue } = useForm<FormData>({
        mode: "all",
        defaultValues,
        resolver: yupResolver(schema),
    });

    const isDefaultOverridden = useWatch({ control, name: "isDefaultOverridden" });

    useEffect(() => {
        trigger();
    }, [trigger]);

    return (
        <div className="vstack gap-3" style={{ maxWidth: 480 }}>
            <div>
                <OverridableField
                    control={control}
                    setValue={setValue}
                    overrideName="isDefaultOverridden"
                    valueName="defaultValue"
                    label="Default"
                    tooltip="Tooltip describing the field"
                >
                    {({ isDisabled }) => (
                        <FormInput
                            type="number"
                            control={control}
                            name="defaultValue"
                            placeholder="Default (30)"
                            disabled={isDisabled}
                        />
                    )}
                </OverridableField>
                <small className="text-muted">Override flag: {isDefaultOverridden ? "on" : "off"}</small>
            </div>
            <OverridableField
                control={control}
                setValue={setValue}
                overrideName="isOverriddenOverridden"
                valueName="overriddenValue"
                label="Overridden"
            >
                {({ isDisabled }) => (
                    <FormInput
                        type="number"
                        control={control}
                        name="overriddenValue"
                        placeholder="Default (30)"
                        disabled={isDisabled}
                    />
                )}
            </OverridableField>
            <OverridableField
                control={control}
                setValue={setValue}
                overrideName="isDisabledOverridden"
                valueName="disabledValue"
                label="Disabled"
                disabled
            >
                {({ isDisabled }) => (
                    <FormInput
                        type="number"
                        control={control}
                        name="disabledValue"
                        placeholder="Default (30)"
                        disabled={isDisabled}
                    />
                )}
            </OverridableField>
            <OverridableField
                control={control}
                setValue={setValue}
                overrideName="isSelectOverridden"
                valueName="selectValue"
                label="Select"
            >
                {({ isDisabled, controlId }) => (
                    <FormSelect
                        control={control}
                        name="selectValue"
                        options={selectOptions}
                        isDisabled={isDisabled}
                        inputId={controlId}
                        placeholder="Default (None)"
                        isSearchable={false}
                    />
                )}
            </OverridableField>
            <OverridableField
                control={control}
                setValue={setValue}
                overrideName="isAddonOverridden"
                valueName="addonValue"
                label="With addon"
            >
                {({ isDisabled }) => (
                    <FormInput
                        type="number"
                        control={control}
                        name="addonValue"
                        placeholder="Default (60)"
                        addon="seconds"
                        disabled={isDisabled}
                    />
                )}
            </OverridableField>
            <OverridableField
                control={control}
                setValue={setValue}
                overrideName="isInvalidOverridden"
                valueName="invalidValue"
                label="Validation error"
            >
                {({ isDisabled }) => (
                    <FormInput
                        type="number"
                        control={control}
                        name="invalidValue"
                        placeholder="Default (8)"
                        disabled={isDisabled}
                    />
                )}
            </OverridableField>
            <OverridableField
                control={control}
                setValue={setValue}
                overrideName="isDurationOverridden"
                valueName="durationValue"
                label="Duration"
            >
                {({ isOverridden, isDisabled, labelId }) => (
                    <FormDurationPicker
                        control={control}
                        name="durationValue"
                        disabled={isDisabled}
                        aria-labelledby={labelId}
                        placeholder={
                            isOverridden
                                ? { hours: "0", minutes: "0", seconds: "0" }
                                : { hours: "Default (0)", minutes: "Default (1)", seconds: "Default (0)" }
                        }
                        showSeconds
                    />
                )}
            </OverridableField>
            <div style={{ width: 240 }}>
                <OverridableField
                    control={control}
                    setValue={setValue}
                    overrideName="isNarrowOverridden"
                    valueName="narrowValue"
                    label="Narrow container (240px)"
                >
                    {({ isDisabled }) => (
                        <FormInput
                            type="number"
                            control={control}
                            name="narrowValue"
                            placeholder="Default (unlimited)"
                            addon="items"
                            disabled={isDisabled}
                        />
                    )}
                </OverridableField>
            </div>
            <OverridableField
                control={control}
                setValue={setValue}
                overrideName="isOptimalOverridden"
                valueName="optimalItems"
                label="With optimal value"
                optimalValue={65536}
                optimalValueLabel="65,536"
                optimalValueHint="Process at most 65,536 items per run to keep each run short."
            >
                {({ isDisabled }) => (
                    <FormInput
                        type="number"
                        control={control}
                        name="optimalItems"
                        placeholder="Default (unlimited)"
                        addon="items"
                        disabled={isDisabled}
                    />
                )}
            </OverridableField>
        </div>
    );
}
