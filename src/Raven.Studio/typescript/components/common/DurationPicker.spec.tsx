import React from "react";
import { within } from "@testing-library/dom";
import { useForm, useWatch } from "react-hook-form";
import { rtlRender } from "test/rtlTestUtils";
import { FormDurationPicker, FormSwitch } from "components/common/Form";

interface DurationFormData {
    isSet: boolean;
    duration: number;
}

function DurationForm({ defaultValues }: { defaultValues: DurationFormData }) {
    const { control, formState, reset } = useForm<DurationFormData>({ defaultValues });
    const { isSet, duration } = useWatch({ control });

    return (
        <div>
            <FormSwitch control={control} name="isSet">
                Set duration
            </FormSwitch>
            {isSet && (
                <div role="group" aria-label="Duration">
                    <FormDurationPicker control={control} name="duration" showDays showSeconds />
                </div>
            )}
            <button type="button" onClick={() => reset()}>
                Reset
            </button>
            <span>{formState.isDirty ? "Form is dirty" : "Form is pristine"}</span>
            <span>Value: {String(duration)}</span>
        </div>
    );
}

describe("DurationPicker", () => {
    it("keeps the form pristine when it mounts without a value", async () => {
        const { screen } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: undefined }} />);

        expect(await screen.findByRole("group", { name: "Duration" })).toBeInTheDocument();
        expect(screen.getByText("Form is pristine")).toBeInTheDocument();
    });

    it("keeps the form pristine when it mounts with a fractional value", async () => {
        const { screen } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 61.5 }} />);

        const durationPicker = await screen.findByRole("group", { name: "Duration" });

        expect(within(durationPicker).getByRole("spinbutton", { name: "Minutes" })).toHaveValue(1);
        expect(within(durationPicker).getByRole("spinbutton", { name: "Seconds" })).toHaveValue(2);
        expect(screen.queryByText("Please enter a whole number")).not.toBeInTheDocument();
        expect(screen.getByText("Form is pristine")).toBeInTheDocument();
    });

    it("keeps the form pristine when it is shown and hidden again", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: false, duration: undefined }} />);

        const toggle = await screen.findByRole("checkbox", { name: "Set duration" });
        await user.click(toggle);
        await user.click(toggle);

        expect(screen.getByText("Form is pristine")).toBeInTheDocument();
    });

    it("clears an invalid value when it is hidden", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 3600 }} />);

        const durationPicker = await screen.findByRole("group", { name: "Duration" });
        const hours = within(durationPicker).getByRole("spinbutton", { name: "Hours" });
        await user.clear(hours);
        await user.type(hours, "1.5");
        expect(screen.getByText("Value: NaN")).toBeInTheDocument();

        await user.click(screen.getByRole("checkbox", { name: "Set duration" }));

        expect(screen.getByText("Value: null")).toBeInTheDocument();
    });

    it("shows empty parts and clears the value when it mounts with an invalid value", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: false, duration: NaN }} />);

        await user.click(await screen.findByRole("checkbox", { name: "Set duration" }));
        const durationPicker = await screen.findByRole("group", { name: "Duration" });

        expect(within(durationPicker).getByRole("spinbutton", { name: "Hours" })).toHaveValue(null);
        expect(screen.queryByText("Please enter a whole number")).not.toBeInTheDocument();
        expect(screen.getByText("Value: null")).toBeInTheDocument();
    });

    it("hides the invalid part message when the value is reset", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 3600 }} />);

        const durationPicker = await screen.findByRole("group", { name: "Duration" });
        const hours = within(durationPicker).getByRole("spinbutton", { name: "Hours" });
        await user.clear(hours);
        await user.type(hours, "1.5");
        expect(screen.getByText("Please enter a whole number")).toBeInTheDocument();

        await user.click(screen.getByRole("button", { name: "Reset" }));

        expect(hours).toHaveValue(1);
        expect(screen.getByText("Value: 3600")).toBeInTheDocument();
        expect(screen.queryByText("Please enter a whole number")).not.toBeInTheDocument();
    });

    it("clears the value once every part is cleared", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 3600 }} />);

        const durationPicker = await screen.findByRole("group", { name: "Duration" });
        for (const part of within(durationPicker).getAllByRole("spinbutton")) {
            await user.clear(part);
        }

        expect(screen.getByText("Value: null")).toBeInTheDocument();
    });
});
