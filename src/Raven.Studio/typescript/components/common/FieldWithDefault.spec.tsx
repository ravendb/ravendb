import React, { useState } from "react";
import { composeStories } from "@storybook/react-webpack5";
import { within } from "@testing-library/dom";
import { useFieldArray, useForm } from "react-hook-form";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import { rtlRender } from "test/rtlTestUtils";
import { FormInputWithDefault, FormSelectWithDefault } from "components/common/FieldWithDefault";
import { SelectOption } from "components/common/select/Select";
import * as stories from "./FieldWithDefault.stories";

const { States } = composeStories(stories);

interface SingleFieldFormData {
    value: number;
}

interface ToolsFormData {
    tools: { isEnabled: boolean }[];
}

const BOOLEAN_OPTIONS: SelectOption<boolean>[] = [
    { label: "True", value: true },
    { label: "False", value: false },
];

function ToolsForm() {
    const { control } = useForm<ToolsFormData>({ defaultValues: { tools: [] } });
    const { fields, append } = useFieldArray({ control, name: "tools" });
    const [isEditing, setIsEditing] = useState(true);

    return (
        <div>
            <Button onClick={() => append({ isEnabled: null })}>Add tool</Button>
            <Button onClick={() => setIsEditing(!isEditing)}>{isEditing ? "Close tools" : "Edit tools"}</Button>
            {isEditing &&
                fields.map((field, index) => (
                    <FormSelectWithDefault
                        key={field.id}
                        control={control}
                        name={`tools.${index}.isEnabled`}
                        label="Enabled"
                        options={BOOLEAN_OPTIONS}
                        defaultValueLabel="True"
                        isSearchable={false}
                    />
                ))}
        </div>
    );
}

function SubmittingForm() {
    const { control, handleSubmit } = useForm<SingleFieldFormData>({
        defaultValues: { value: 45 },
    });

    return (
        <Form onSubmit={handleSubmit(() => new Promise<void>(() => {}))}>
            <FormInputWithDefault control={control} name="value" label="Custom" type="number" defaultValueLabel="10" />
            <Button type="submit">Save</Button>
        </Form>
    );
}

function DirtyTrackingForm({ onValueChange }: { onValueChange?: (value: number) => void }) {
    const { control, formState } = useForm<SingleFieldFormData>({
        defaultValues: { value: null },
    });

    return (
        <div>
            <FormInputWithDefault
                control={control}
                name="value"
                label="Value"
                optimalValue={100}
                afterChange={onValueChange}
                type="number"
                defaultValueLabel="10"
            />
            <span>{formState.isDirty ? "Form is dirty" : "Form is pristine"}</span>
        </div>
    );
}

describe("FieldWithDefault", () => {
    it("keeps the input editable while it uses the default", async () => {
        const { screen } = rtlRender(<States />);

        expect(await screen.findByName("defaultValue")).toBeEnabled();
        expect(screen.queryByRole("button", { name: "Reset to default", description: "Default" })).toBeNull();
    });

    it("offers the reset once a value is typed", async () => {
        const { screen, user } = rtlRender(<States />);

        await user.type(await screen.findByName("defaultValue"), "45");

        expect(screen.getByRole("button", { name: "Reset to default", description: "Default" })).toBeInTheDocument();
    });

    it("goes back to the default when the value is cleared", async () => {
        const { screen, user } = rtlRender(<States />);

        await user.type(await screen.findByName("defaultValue"), "45");
        await user.clear(screen.getByName("defaultValue"));

        expect(screen.queryByRole("button", { name: "Reset to default", description: "Default" })).toBeNull();
    });

    it("resets the value to the default", async () => {
        const { screen, user } = rtlRender(<States />);

        expect(await screen.findByName("customValue")).toHaveValue(45);

        await user.click(screen.getByRole("button", { name: "Reset to default", description: "Custom" }));

        expect(screen.getByName("customValue")).toHaveValue(null);
        expect(screen.queryByRole("button", { name: "Reset to default", description: "Custom" })).toBeNull();
    });

    it("is not dirty after a typed value is cleared", async () => {
        const { screen, user } = rtlRender(<DirtyTrackingForm />);

        await user.type(await screen.findByName("value"), "5");
        expect(screen.getByText("Form is dirty")).toBeInTheDocument();

        await user.clear(screen.getByName("value"));

        expect(screen.getByText("Form is pristine")).toBeInTheDocument();
    });

    it("is not dirty after a typed value is reset to the default", async () => {
        const { screen, user } = rtlRender(<DirtyTrackingForm />);

        await user.type(await screen.findByName("value"), "5");
        await user.click(screen.getByRole("button", { name: "Reset to default" }));

        expect(screen.getByText("Form is pristine")).toBeInTheDocument();
    });

    it("keeps a False selection instead of treating it as the default", async () => {
        const { screen, user } = rtlRender(<ToolsForm />);

        await user.click(await screen.findByRole("button", { name: "Add tool" }));
        await user.click(screen.getByRole("combobox", { name: "Enabled" }));
        await user.click(await screen.findByText("False"));

        expect(screen.getByText("False")).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Reset to default", description: "Enabled" })).toBeInTheDocument();
    });

    it("resets a newly added list item after it is shown again", async () => {
        const { screen, user } = rtlRender(<ToolsForm />);

        await user.click(await screen.findByRole("button", { name: "Add tool" }));
        await user.click(screen.getByRole("combobox", { name: "Enabled" }));
        await user.click(await screen.findByText("False"));
        await user.click(screen.getByRole("button", { name: "Close tools" }));
        await user.click(screen.getByRole("button", { name: "Edit tools" }));

        await user.click(screen.getByRole("button", { name: "Reset to default", description: "Enabled" }));

        expect(screen.queryByText("False")).not.toBeInTheDocument();
        expect(screen.getByText("Default (True)")).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: "Reset to default" })).not.toBeInTheDocument();
    });

    it("shows the default value in the placeholder", async () => {
        const { screen } = rtlRender(<DirtyTrackingForm />);

        expect(await screen.findByName("value")).toHaveAttribute("placeholder", "Default (10)");
    });

    it("reports the values it sets", async () => {
        const onValueChange = jest.fn();
        const { screen, user } = rtlRender(<DirtyTrackingForm onValueChange={onValueChange} />);

        await user.click(await screen.findByRole("button", { name: "Use 100" }));
        await user.click(screen.getByRole("button", { name: "Reset to default" }));

        expect(onValueChange.mock.calls).toEqual([[100], [null]]);
    });

    it("goes back to the default when a text value is cleared", async () => {
        const { screen, user } = rtlRender(<States />);

        await user.type(await screen.findByName("textValue"), "-");
        expect(screen.getByRole("button", { name: "Reset to default", description: "Text" })).toBeInTheDocument();

        await user.clear(screen.getByName("textValue"));

        expect(screen.queryByRole("button", { name: "Reset to default", description: "Text" })).toBeNull();
    });

    it("reports user edits too", async () => {
        const onValueChange = jest.fn();
        const { screen, user } = rtlRender(<DirtyTrackingForm onValueChange={onValueChange} />);

        await user.type(await screen.findByName("value"), "7");

        expect(onValueChange).toHaveBeenLastCalledWith(7);
    });

    it("disables the input and hides the reset when disabled", async () => {
        const { screen } = rtlRender(<States />);

        expect(await screen.findByName("disabledValue")).toBeDisabled();
        expect(screen.queryByRole("button", { name: "Reset to default", description: "Disabled" })).toBeNull();
    });

    it("focuses the input when its label is clicked", async () => {
        const { screen, user } = rtlRender(<States />);

        await user.click(await screen.findByText("With addon"));

        expect(screen.getByName("addonValue")).toHaveFocus();
    });

    it("focuses the select when its label is clicked", async () => {
        const { screen, user } = rtlRender(<States />);

        await user.click(await screen.findByText("Select"));

        expect(screen.getByRole("combobox", { name: "Select" })).toHaveFocus();
    });

    it("names the duration picker after the field", async () => {
        const { screen } = rtlRender(<States />);

        const durationPicker = await screen.findByRole("group", { name: "Duration" });

        expect(within(durationPicker).getByRole("spinbutton", { name: "Hours" })).toHaveValue(1);
        expect(within(durationPicker).getByRole("spinbutton", { name: "Minutes" })).toHaveValue(0);
        expect(within(durationPicker).getByRole("spinbutton", { name: "Seconds" })).toHaveValue(0);
    });

    it("focuses the first duration part when its label is clicked", async () => {
        const { screen, user } = rtlRender(<States />);

        const durationPicker = await screen.findByRole("group", { name: "Duration" });
        await user.click(screen.getByText("Duration", { selector: "label" }));

        expect(within(durationPicker).getByRole("spinbutton", { name: "Hours" })).toHaveFocus();
    });

    it("shows the default duration once every part is cleared", async () => {
        const { screen, user } = rtlRender(<States />);

        const durationPicker = await screen.findByRole("group", { name: "Duration" });
        for (const part of within(durationPicker).getAllByRole("spinbutton")) {
            await user.clear(part);
        }

        expect(screen.queryByRole("button", { name: "Reset to default", description: "Duration" })).toBeNull();
        expect(within(durationPicker).getByRole("spinbutton", { name: "Minutes" })).toHaveAttribute(
            "placeholder",
            "Default (1)"
        );
    });

    it("shows zero placeholders while a duration is set", async () => {
        const { screen, user } = rtlRender(<States />);

        const durationPicker = await screen.findByRole("group", { name: "Duration" });
        await user.clear(within(durationPicker).getByRole("spinbutton", { name: "Minutes" }));

        expect(within(durationPicker).getByRole("spinbutton", { name: "Minutes" })).toHaveAttribute("placeholder", "0");
    });

    it.each([
        ["-5", "Please enter a non-negative number"],
        ["1.5", "Please enter a whole number"],
    ])("rejects a duration part of %s even when the total is positive", async (minutes, message) => {
        const { screen, user } = rtlRender(<States />);

        const durationPicker = await screen.findByRole("group", { name: "Duration" });
        const minutesInput = within(durationPicker).getByRole("spinbutton", { name: "Minutes" });
        await user.clear(minutesInput);
        await user.type(minutesInput, minutes);

        expect(await screen.findByText(message)).toBeInTheDocument();

        await user.clear(minutesInput);
        await user.type(minutesInput, "5");

        expect(screen.queryByText(message)).toBeNull();
        expect(within(durationPicker).getByRole("spinbutton", { name: "Hours" })).toHaveValue(1);
    });

    it("switches the message when a negative part becomes a fraction", async () => {
        const { screen, user } = rtlRender(<States />);

        const durationPicker = await screen.findByRole("group", { name: "Duration" });
        const minutesInput = within(durationPicker).getByRole("spinbutton", { name: "Minutes" });
        await user.clear(minutesInput);
        await user.type(minutesInput, "-5");
        expect(await screen.findByText("Please enter a non-negative number")).toBeInTheDocument();

        await user.clear(minutesInput);
        await user.type(minutesInput, "0.5");

        expect(await screen.findByText("Please enter a whole number")).toBeInTheDocument();
        expect(screen.queryByText("Please enter a non-negative number")).toBeNull();
    });

    it("suggests the optimal value while it is not in use", async () => {
        const { screen } = rtlRender(<States />);

        expect(
            await screen.findByText("Process at most 65,536 items per run to keep each run short.")
        ).toBeInTheDocument();
        expect(
            screen.getByRole("button", { name: "Use 65,536", description: "With optimal value" })
        ).toBeInTheDocument();
    });

    it("uses the optimal value and hides the suggestion", async () => {
        const { screen, user } = rtlRender(<States />);

        await user.click(await screen.findByRole("button", { name: "Use 65,536", description: "With optimal value" }));

        expect(screen.getByName("optimalItems")).toHaveValue(65536);
        expect(screen.getByText("Optimal", { selector: ".badge" })).toBeInTheDocument();
        expect(screen.queryByText("Process at most 65,536 items per run to keep each run short.")).toBeNull();
        expect(
            screen.getByRole("button", { name: "Reset to default", description: "With optimal value" })
        ).toBeInTheDocument();
    });

    it("hides the optimal suggestion while the value is invalid", async () => {
        const { screen, user } = rtlRender(<States />);

        await user.type(await screen.findByName("optimalItems"), "0");

        expect(await screen.findByText("Enter at least 1 item")).toBeInTheDocument();
        expect(screen.queryByText("Process at most 65,536 items per run to keep each run short.")).toBeNull();
    });

    it("suggests the optimal value again once another value is typed", async () => {
        const { screen, user } = rtlRender(<States />);

        await user.click(await screen.findByRole("button", { name: "Use 65,536", description: "With optimal value" }));
        await user.clear(screen.getByName("optimalItems"));
        await user.type(screen.getByName("optimalItems"), "1000");

        expect(screen.getByText("Process at most 65,536 items per run to keep each run short.")).toBeInTheDocument();
        expect(screen.queryByText("Optimal", { selector: ".badge" })).toBeNull();
    });

    it("locks the reset while the form is submitting", async () => {
        const { screen, user } = rtlRender(<SubmittingForm />);

        const resetButton = await screen.findByRole("button", { name: "Reset to default" });
        expect(resetButton).toBeEnabled();

        await user.click(screen.getByRole("button", { name: "Save" }));

        expect(resetButton).toBeDisabled();
    });
});
