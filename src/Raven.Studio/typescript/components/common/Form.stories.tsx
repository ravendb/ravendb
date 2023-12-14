import { Meta } from "@storybook/react";
import {
    FormInput,
    FormCheckbox,
    FormCheckboxes,
    FormRadio,
    FormDatePicker,
    FormDurationPicker,
    FormRadioToggleWithIcon,
    FormSelect,
    FormSelectCreatable,
    FormSwitch,
    FormAceEditor,
} from "./Form";
import React, { useEffect } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { useForm } from "react-hook-form";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { Label } from "reactstrap";

export default {
    title: "Bits/Form",
    component: Form,
    decorators: [withStorybookContexts, withBootstrap5],
    args: {
        isDefaultValid: false,
    },
} satisfies Meta;

export function Form({ isDefaultValid }: { isDefaultValid: boolean }) {
    const { control, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues: isDefaultValid ? validValues : invalidValues,
        resolver: formResolver,
    });

    useEffect(() => {
        trigger();
    }, [trigger]);

    return (
        <div className="vstack gap-2 w-50">
            <Label>
                Input text
                <FormInput type="text" control={control} name="inputText" />
            </Label>
            <Label>
                Input number
                <FormInput type="number" control={control} name="inputNumber" />
            </Label>
            <FormCheckbox control={control} name="inputCheckbox">
                Checkbox
            </FormCheckbox>
            <Label>
                Checkboxes
                <FormCheckboxes
                    control={control}
                    name="inputCheckboxes"
                    options={[
                        { label: "Option 1", value: true },
                        { label: "Option 2", value: false },
                    ]}
                />
            </Label>
            <FormSwitch control={control} name="inputSwitch">
                Switch
            </FormSwitch>
            <FormRadio control={control} name="inputRadio">
                Radio
            </FormRadio>
            <Label>
                Radio toggle with icon
                <FormRadioToggleWithIcon
                    control={control}
                    name="inputRadioToggleWithIcon"
                    leftItem={{ label: "True", value: true, iconName: "check" }}
                    rightItem={{ label: "False", value: false, iconName: "cancel" }}
                />
            </Label>
            <Label>
                Select
                <FormSelect
                    control={control}
                    name="inputSelect"
                    options={[
                        { label: "Option 1", value: 1 },
                        { label: "Option 2", value: 2 },
                    ]}
                />
            </Label>
            <Label>
                Select creatable
                <FormSelectCreatable
                    control={control}
                    name="inputSelectCreatable"
                    options={[
                        { label: "Option 1", value: 1 },
                        { label: "Option 2", value: 2 },
                    ]}
                />
            </Label>
            <Label>
                Date picker
                <FormDatePicker control={control} name="inputDatePicker" />
            </Label>
            <Label>
                Duration picker
                <FormDurationPicker control={control} name="inputDurationPicker" />
            </Label>
            <Label>
                Ace editor
                <FormAceEditor mode="javascript" control={control} name="inputAceEditor" />
            </Label>
        </div>
    );
}

const schema = yup.object().shape({
    inputText: yup.string().required(),
    inputNumber: yup.number().required().positive(),
    inputCheckbox: yup.boolean().oneOf([true]),
    inputCheckboxes: yup.array().of(yup.boolean().oneOf([true])),
    inputSwitch: yup.boolean().oneOf([true]),
    inputRadio: yup.boolean().oneOf([true]),
    inputRadioToggleWithIcon: yup.boolean().oneOf([true]),
    inputSelect: yup.number().nullable().required(),
    inputSelectCreatable: yup.number().nullable().required(),
    inputDatePicker: yup.date().required(),
    inputDurationPicker: yup.number().required(),
    inputAceEditor: yup.string().required(),
});

const formResolver = yupResolver(schema);
type FormData = yup.InferType<typeof schema>;

const validValues: FormData = {
    inputText: "text",
    inputNumber: 2,
    inputCheckbox: true,
    inputCheckboxes: [true, false],
    inputRadio: true,
    inputSelect: 1,
    inputSelectCreatable: 1,
    inputDatePicker: new Date(),
    inputDurationPicker: 2,
    inputAceEditor: "const x = 1;",
};

const invalidValues: FormData = {
    inputText: "",
    inputNumber: -2,
    inputCheckbox: false,
    inputCheckboxes: [false, false],
    inputRadio: false,
    inputSelect: null,
    inputSelectCreatable: null,
    inputDatePicker: null,
    inputDurationPicker: null,
    inputAceEditor: "",
};
