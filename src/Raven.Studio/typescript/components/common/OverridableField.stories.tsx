import { Meta } from "@storybook/react-webpack5";
import { useEffect } from "react";
import { useForm } from "react-hook-form";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { FormInput, FormSelect } from "components/common/Form";
import OverridableField from "components/common/OverridableField";
import { SelectOption } from "components/common/select/Select";

export default {
    title: "Bits/OverridableField",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

const schema = yup.object({
    isOffOverridden: yup.boolean(),
    offValue: yup.number().nullable(),
    isOnOverridden: yup.boolean(),
    onValue: yup.number().nullable(),
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
});

type FormData = yup.InferType<typeof schema>;

const defaultValues: FormData = {
    isOffOverridden: false,
    offValue: null,
    isOnOverridden: true,
    onValue: 30,
    isDisabledOverridden: false,
    disabledValue: null,
    isSelectOverridden: false,
    selectValue: null,
    isAddonOverridden: true,
    addonValue: 60,
    isInvalidOverridden: true,
    invalidValue: 0,
    isNarrowOverridden: false,
    narrowValue: null,
};

const selectOptions: SelectOption[] = [
    { label: "None", value: "None" },
    { label: "Round Robin", value: "RoundRobin" },
    { label: "Fastest Node", value: "FastestNode" },
];

export function States() {
    const { control, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues,
        resolver: yupResolver(schema),
    });

    useEffect(() => {
        trigger();
    }, [trigger]);

    return (
        <div className="vstack gap-3" style={{ maxWidth: 480 }}>
            <OverridableField control={control} overrideName="isOffOverridden" label="Override off">
                {({ isDisabled }) => (
                    <FormInput
                        type="number"
                        control={control}
                        name="offValue"
                        placeholder="Default (30)"
                        disabled={isDisabled}
                    />
                )}
            </OverridableField>
            <OverridableField
                control={control}
                overrideName="isOnOverridden"
                label="Override on"
                tooltip="Tooltip describing the field"
            >
                {({ isDisabled }) => (
                    <FormInput
                        type="number"
                        control={control}
                        name="onValue"
                        placeholder="Default (30)"
                        disabled={isDisabled}
                    />
                )}
            </OverridableField>
            <OverridableField control={control} overrideName="isDisabledOverridden" label="Disabled" disabled>
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
            <OverridableField control={control} overrideName="isSelectOverridden" label="Select">
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
            <OverridableField control={control} overrideName="isAddonOverridden" label="With addon">
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
            <OverridableField control={control} overrideName="isInvalidOverridden" label="Validation error">
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
            <div style={{ width: 240 }}>
                <OverridableField control={control} overrideName="isNarrowOverridden" label="Narrow container (240px)">
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
        </div>
    );
}
