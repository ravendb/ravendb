import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { within } from "@testing-library/dom";
import { useForm } from "react-hook-form";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import { rtlRender } from "test/rtlTestUtils";
import { FormInput } from "components/common/Form";
import OverridableField from "components/common/OverridableField";
import * as stories from "./OverridableField.stories";

const { States } = composeStories(stories);

interface SubmittingFormData {
    isOverridden: boolean;
    value: number;
}

function SubmittingForm() {
    const { control, handleSubmit, setValue } = useForm<SubmittingFormData>({
        defaultValues: { isOverridden: true, value: 45 },
    });

    return (
        <Form onSubmit={handleSubmit(() => new Promise<void>(() => {}))}>
            <OverridableField
                control={control}
                setValue={setValue}
                overrideName="isOverridden"
                valueName="value"
                label="Overridden"
            >
                {({ isDisabled }) => <FormInput type="number" control={control} name="value" disabled={isDisabled} />}
            </OverridableField>
            <Button type="submit">Save</Button>
        </Form>
    );
}

describe("OverridableField", () => {
    it("keeps the input editable while it uses the default", async () => {
        const { screen } = rtlRender(<States />);

        expect(await screen.findByName("defaultValue")).toBeEnabled();
        expect(screen.queryByRole("button", { name: "Reset to default", description: "Default" })).toBeNull();
    });

    it("overrides the default once a value is typed", async () => {
        const { screen, user } = rtlRender(<States />);

        expect(await screen.findByText("Override flag: off")).toBeInTheDocument();

        await user.type(screen.getByName("defaultValue"), "45");

        expect(screen.getByText("Override flag: on")).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Reset to default", description: "Default" })).toBeInTheDocument();
    });

    it("goes back to the default when the value is cleared", async () => {
        const { screen, user } = rtlRender(<States />);

        await user.type(await screen.findByName("defaultValue"), "45");
        await user.clear(screen.getByName("defaultValue"));

        expect(screen.getByText("Override flag: off")).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: "Reset to default", description: "Default" })).toBeNull();
    });

    it("resets the value to the default", async () => {
        const { screen, user } = rtlRender(<States />);

        expect(await screen.findByName("overriddenValue")).toHaveValue(45);

        await user.click(screen.getByRole("button", { name: "Reset to default", description: "Overridden" }));

        expect(screen.getByName("overriddenValue")).toHaveValue(null);
        expect(screen.queryByRole("button", { name: "Reset to default", description: "Overridden" })).toBeNull();
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
