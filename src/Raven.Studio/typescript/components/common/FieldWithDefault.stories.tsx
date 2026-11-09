import { Meta } from "@storybook/react-webpack5";
import { useEffect } from "react";
import { useForm } from "react-hook-form";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import {
    FormDurationPickerWithDefault,
    FormInputWithDefault,
    FormSelectWithDefault,
} from "components/common/FieldWithDefault";
import { SelectOption } from "components/common/select/Select";

export default {
    title: "Bits/FieldWithDefault",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

const schema = yup.object({
    defaultValue: yup.number().nullable(),
    customValue: yup.number().nullable(),
    disabledValue: yup.number().nullable(),
    textValue: yup.string().nullable(),
    selectValue: yup.string().nullable(),
    addonValue: yup.number().nullable(),
    invalidValue: yup.number().nullable().min(1, "Value must be at least 1"),
    narrowValue: yup.number().nullable(),
    durationValue: yup.number().nullable(),
    optimalItems: yup.number().nullable().min(1, "Enter at least 1 item"),
});

type FormData = yup.InferType<typeof schema>;

const DEFAULT_VALUES: FormData = {
    defaultValue: null,
    customValue: 45,
    disabledValue: 30,
    textValue: null,
    selectValue: null,
    addonValue: 60,
    invalidValue: 0,
    narrowValue: null,
    durationValue: 3600,
    optimalItems: null,
};

const SELECT_OPTIONS: SelectOption[] = [
    { label: "None", value: "None" },
    { label: "Round Robin", value: "RoundRobin" },
    { label: "Fastest Node", value: "FastestNode" },
];

export function States() {
    const { control, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues: DEFAULT_VALUES,
        resolver: yupResolver(schema),
    });

    useEffect(() => {
        trigger();
    }, [trigger]);

    return (
        <div className="vstack gap-3" style={{ maxWidth: 480 }}>
            <FormInputWithDefault
                control={control}
                name="defaultValue"
                label="Default"
                tooltip="Tooltip describing the field"
                type="number"
                defaultValueLabel="30"
            />
            <FormInputWithDefault
                control={control}
                name="customValue"
                label="Custom"
                type="number"
                defaultValueLabel="30"
            />
            <FormInputWithDefault
                control={control}
                name="disabledValue"
                label="Disabled"
                disabled
                type="number"
                defaultValueLabel="30"
            />
            <FormInputWithDefault control={control} name="textValue" label="Text" type="text" defaultValueLabel="/" />
            <FormSelectWithDefault
                control={control}
                name="selectValue"
                label="Select"
                options={SELECT_OPTIONS}
                defaultValueLabel="None"
                isSearchable={false}
            />
            <FormInputWithDefault
                control={control}
                name="addonValue"
                label="With addon"
                type="number"
                defaultValueLabel="60"
                addon="seconds"
            />
            <FormInputWithDefault
                control={control}
                name="invalidValue"
                label="Validation error"
                type="number"
                defaultValueLabel="8"
            />
            <FormDurationPickerWithDefault
                control={control}
                name="durationValue"
                label="Duration"
                defaultTotalSeconds={60}
                showSeconds
            />
            <div style={{ width: 240 }}>
                <FormInputWithDefault
                    control={control}
                    name="narrowValue"
                    label="Narrow container (240px)"
                    type="number"
                    defaultValueLabel="unlimited"
                    addon="items"
                />
            </div>
            <FormInputWithDefault
                control={control}
                name="optimalItems"
                label="With optimal value"
                optimalValue={65536}
                optimalValueLabel="65,536"
                optimalValueHint="Process at most 65,536 items per run to keep each run short."
                type="number"
                defaultValueLabel="unlimited"
                addon="items"
            />
        </div>
    );
}
