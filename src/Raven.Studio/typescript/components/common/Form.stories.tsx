import { Meta } from "@storybook/react";
import {
    FormAceEditor,
    FormCheckbox,
    FormCheckboxes,
    FormDatePicker,
    FormDurationPicker,
    FormGroup,
    FormInput,
    FormLabel,
    FormPathSelector,
    FormRadio,
    FormRadioToggleWithIcon,
    FormSelect,
    FormSelectCreatable,
    FormSwitch,
} from "./Form";
import { useEffect } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { useForm } from "react-hook-form";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import InputGroup from "react-bootstrap/InputGroup";
import ReactBootstrapForm from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";

export default {
    title: "Bits/Form",
    component: Form,
    decorators: [withStorybookContexts, withBootstrap5],
    args: {
        isDefaultValid: true,
    },
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=8-2111",
        },
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
            <FormGroup>
                <FormLabel>Input text</FormLabel>
                <FormInput type="text" control={control} name="inputText" />
            </FormGroup>
            <FormGroup>
                <InputGroup>
                    <InputGroup.Text>@</InputGroup.Text>
                    <ReactBootstrapForm.Control placeholder="username" />
                </InputGroup>
                <br />
                <InputGroup>
                    <InputGroup.Text>
                        <FormCheckbox control={control} name="inputGroupCheckbox" />
                    </InputGroup.Text>
                    <ReactBootstrapForm.Control placeholder="Check it out" />
                </InputGroup>
                <br />
                <InputGroup>
                    <ReactBootstrapForm.Control placeholder="username" />
                    <InputGroup.Text>@example.com</InputGroup.Text>
                </InputGroup>
                <br />
                <InputGroup>
                    <InputGroup.Text>$</InputGroup.Text>
                    <InputGroup.Text>$</InputGroup.Text>
                    <ReactBootstrapForm.Control placeholder="Dolla dolla billz yo!" />
                    <InputGroup.Text>$</InputGroup.Text>
                    <InputGroup.Text>$</InputGroup.Text>
                </InputGroup>
            </FormGroup>
            <FormGroup>
                <FormLabel>Input number</FormLabel>
                <FormInput type="number" control={control} name="inputNumber" />
            </FormGroup>
            <FormGroup>
                <FormLabel>Input with password preview</FormLabel>
                <FormInput type="password" control={control} name="inputPasswordPreview" passwordPreview />
            </FormGroup>
            <FormGroup>
                <FormCheckbox control={control} name="inputCheckbox">
                    Checkbox
                </FormCheckbox>
            </FormGroup>
            <FormGroup className="mt-2">
                <FormLabel>Checkboxes</FormLabel>
                <FormCheckboxes
                    control={control}
                    name="inputCheckboxes"
                    options={[
                        { label: "Option 1", value: true },
                        { label: "Option 2", value: false },
                    ]}
                />
            </FormGroup>
            <FormGroup>
                <FormSwitch control={control} name="inputSwitch">
                    Switch
                </FormSwitch>
            </FormGroup>
            <FormGroup>
                <FormRadio control={control} name="inputRadio">
                    Radio
                </FormRadio>
            </FormGroup>
            <FormGroup>
                <FormLabel>Radio toggle with icon</FormLabel>
                <FormRadioToggleWithIcon
                    control={control}
                    name="inputRadioToggleWithIcon"
                    leftItem={{ label: "True", value: true, iconName: "check" }}
                    rightItem={{ label: "False", value: false, iconName: "cancel" }}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>Select</FormLabel>
                <FormSelect
                    control={control}
                    name="inputSelect"
                    options={[
                        { label: "Option 1", value: 1 },
                        { label: "Option 2", value: 2 },
                    ]}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>Select creatable</FormLabel>
                <FormSelectCreatable
                    control={control}
                    name="inputSelectCreatable"
                    options={[
                        { label: "Option 1", value: 1 },
                        { label: "Option 2", value: 2 },
                    ]}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>Multi select</FormLabel>
                <FormSelect
                    control={control}
                    name="inputMultiSelect"
                    options={[
                        { label: "Option 1", value: 1 },
                        { label: "Option 2", value: 2 },
                    ]}
                    isMulti
                />
            </FormGroup>

            <FormGroup>
                <InputGroup>
                    <FormInput type="text" control={control} name="inputText" />
                    <Button variant="secondary" title="Test connection">
                        <Icon icon="rocket" />
                        Test connection
                    </Button>
                </InputGroup>
            </FormGroup>

            <FormGroup>
                <FormLabel>Date picker</FormLabel>
                <FormDatePicker control={control} name="inputDatePicker" />
            </FormGroup>
            <FormGroup>
                <FormLabel>Duration picker</FormLabel>
                <FormDurationPicker control={control} name="inputDurationPicker" />
            </FormGroup>
            <FormGroup>
                <FormLabel>Ace editor</FormLabel>
                <FormAceEditor mode="javascript" control={control} name="inputAceEditor" />
            </FormGroup>
            <FormGroup>
                <FormLabel>Path selector</FormLabel>
                <FormPathSelector
                    control={control}
                    name="inputPath"
                    getPathsProvider={() => () => Promise.resolve(["C:\\", "D:\\"])}
                    getPathDependencies={(path: string) => [path]}
                />
            </FormGroup>
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
