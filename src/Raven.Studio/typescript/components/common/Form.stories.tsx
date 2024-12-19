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
    FormPathSelector,
} from "./Form";
import React, { useEffect } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { useForm } from "react-hook-form";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { Button, Input, InputGroup, InputGroupText, Label } from "reactstrap";
import { Checkbox } from "components/common/Checkbox";

export default {
    title: "Bits/Form",
    component: Form,
    decorators: [withStorybookContexts, withBootstrap5],
    args: {
        isDefaultValid: true,
    },
} satisfies Meta;

export function Form({ isDefaultValid }: { isDefaultValid: boolean }) {
    const defaultValues = isDefaultValid ? validValues : invalidValues;

    const { control, trigger, reset } = useForm<FormData>({
        mode: "all",
        defaultValues,
        resolver: formResolver,
    });

    useEffect(() => {
        reset(defaultValues);
        trigger();
    }, [isDefaultValid, reset, defaultValues, trigger]);

    return (
        <div className="vstack gap-4 w-50">
            <div>
                <Label>Input text</Label>
                <FormInput type="text" control={control} name="inputText" />
            </div>
            <div>
                <InputGroup>
                    <InputGroupText>@</InputGroupText>
                    <Input placeholder="username" />
                </InputGroup>
                <br />
                <InputGroup>
                    <InputGroupText>
                        <FormCheckbox control={control} name="inputGroupCheckbox" />
                    </InputGroupText>
                    <Input placeholder="Check it out" />
                </InputGroup>
                <br />
                <InputGroup>
                    <Input placeholder="username" />
                    <InputGroupText>@example.com</InputGroupText>
                </InputGroup>
                <br />
                <InputGroup>
                    <InputGroupText>$</InputGroupText>
                    <InputGroupText>$</InputGroupText>
                    <Input placeholder="Dolla dolla billz yo!" />
                    <InputGroupText>$</InputGroupText>
                    <InputGroupText>$</InputGroupText>
                </InputGroup>
            </div>
            <div>
                <Label>Input number</Label>
                <FormInput type="number" control={control} name="inputNumber" />
            </div>
            <div>
                <Label>Input with password preview</Label>
                <FormInput type="password" control={control} name="inputPasswordPreview" passwordPreview />
            </div>
            <div className="mt-3">
                <FormCheckbox control={control} name="inputCheckbox">
                    Checkbox
                </FormCheckbox>
            </div>
            <div className="mt-2">
                <Label>Checkboxes</Label>
                <FormCheckboxes
                    control={control}
                    name="inputCheckboxes"
                    options={[
                        { label: "Option 1", value: true },
                        { label: "Option 2", value: false },
                    ]}
                />
            </div>
            <div>
                <FormSwitch control={control} name="inputSwitch">
                    Switch
                </FormSwitch>
            </div>
            <div>
                <FormRadio control={control} name="inputRadio">
                    Radio
                </FormRadio>
            </div>
            <div>
                <Label>Radio toggle with icon</Label>
                <FormRadioToggleWithIcon
                    control={control}
                    name="inputRadioToggleWithIcon"
                    leftItem={{ label: "True", value: true, iconName: "check" }}
                    rightItem={{ label: "False", value: false, iconName: "cancel" }}
                />
            </div>
            <div>
                <Label>Select</Label>
                <FormSelect
                    control={control}
                    name="inputSelect"
                    options={[
                        { label: "Option 1", value: 1 },
                        { label: "Option 2", value: 2 },
                    ]}
                />
            </div>
            <div>
                <Label>Select creatable</Label>
                <FormSelectCreatable
                    control={control}
                    name="inputSelectCreatable"
                    options={[
                        { label: "Option 1", value: 1 },
                        { label: "Option 2", value: 2 },
                    ]}
                />
            </div>
            <div>
                <Label>Multi select</Label>
                <FormSelect
                    control={control}
                    name="inputMultiSelect"
                    options={[
                        { label: "Option 1", value: 1 },
                        { label: "Option 2", value: 2 },
                    ]}
                    isMulti
                />
            </div>

            <div className="input-group">
                <FormInput type="text" control={control} name="inputText" />
                <Button color="secondary" icon="rocket" title="Test connection">
                    Test connection
                </Button>
            </div>

            <div>
                <Label>Date picker</Label>
                <FormDatePicker control={control} name="inputDatePicker" />
            </div>
            <div>
                <Label>Duration picker</Label>
                <FormDurationPicker control={control} name="inputDurationPicker" />
            </div>
            <div>
                <Label>Ace editor</Label>
                <FormAceEditor mode="javascript" control={control} name="inputAceEditor" />
            </div>
            <div>
                <Label>Path selector</Label>
                <FormPathSelector
                    control={control}
                    name="inputPath"
                    getPaths={() => Promise.resolve(["C:\\", "D:\\"])}
                    getPathDependencies={(path: string) => [path]}
                />
            </div>
        </div>
    );
}

const schema = yup.object().shape({
    inputText: yup.string().required(),
    inputNumber: yup.number().required().positive(),
    inputPasswordPreview: yup.string().required(),
    inputCheckbox: yup.boolean().oneOf([true]),
    inputGroupCheckbox: yup.boolean().oneOf([true]),
    inputCheckboxes: yup.array().of(yup.boolean().oneOf([true])),
    inputSwitch: yup.boolean().oneOf([true]),
    inputRadio: yup.boolean().oneOf([true]),
    inputRadioToggleWithIcon: yup.boolean().oneOf([true]),
    inputSelect: yup.number().nullable().required(),
    inputMultiSelect: yup.number().nullable().required(),
    inputSelectCreatable: yup.number().nullable().required(),
    inputDatePicker: yup.date().required(),
    inputDurationPicker: yup.number().required(),
    inputAceEditor: yup.string().required(),
    inputPath: yup.string().required(),
});

const formResolver = yupResolver(schema);
type FormData = yup.InferType<typeof schema>;

const validValues: FormData = {
    inputText: "text",
    inputNumber: 2,
    inputPasswordPreview: "password",
    inputCheckbox: true,
    inputGroupCheckbox: true,
    inputCheckboxes: [true, false],
    inputRadio: true,
    inputSelect: 1,
    inputMultiSelect: 1,
    inputSelectCreatable: 1,
    inputDatePicker: new Date(),
    inputDurationPicker: 2,
    inputAceEditor: "const x = 1;",
    inputPath: "C:\\",
};

const invalidValues: FormData = {
    inputText: "",
    inputNumber: -2,
    inputPasswordPreview: "",
    inputCheckbox: false,
    inputGroupCheckbox: false,
    inputCheckboxes: [false, false],
    inputRadio: false,
    inputSelect: null,
    inputMultiSelect: null,
    inputSelectCreatable: null,
    inputDatePicker: null,
    inputDurationPicker: null,
    inputAceEditor: "",
    inputPath: "",
};
