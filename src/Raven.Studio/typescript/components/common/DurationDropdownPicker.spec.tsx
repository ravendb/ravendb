import React from "react";
import { within } from "@testing-library/dom";
import { useForm, useWatch } from "react-hook-form";
import { rtlRender } from "test/rtlTestUtils";
import { FormDurationPicker, FormSwitch } from "components/common/Form";

interface DurationFormData {
    isSet: boolean;
    duration: number;
}

function DurationForm({
    defaultValues,
    defaultTotalSeconds,
}: {
    defaultValues: DurationFormData;
    defaultTotalSeconds?: number;
}) {
    const { control, formState, reset } = useForm<DurationFormData>({ defaultValues });
    const { isSet, duration } = useWatch({ control });

    return (
        <div>
            <FormSwitch control={control} name="isSet">
                Set duration
            </FormSwitch>
            {isSet && (
                <div role="group" aria-label="Duration">
                    <FormDurationPicker
                        control={control}
                        name="duration"
                        showDays
                        showSeconds
                        defaultTotalSeconds={defaultTotalSeconds}
                    />
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

async function findDurationInput(screen: ReturnType<typeof rtlRender>["screen"]) {
    return within(await screen.findByRole("group", { name: "Duration" })).getByRole("textbox");
}

describe("DurationDropdownPicker", () => {
    it("keeps the form pristine when it mounts without a value", async () => {
        const { screen } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: undefined }} />);

        expect(await findDurationInput(screen)).toHaveValue("");
        expect(screen.getByText("Form is pristine")).toBeInTheDocument();
    });

    it("keeps the form pristine when it mounts with a fractional value", async () => {
        const { screen } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 61.5 }} />);

        expect(await findDurationInput(screen)).toHaveValue("1m 2s");
        expect(screen.getByText("Form is pristine")).toBeInTheDocument();
    });

    it("keeps the form pristine when it is shown and hidden again", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: false, duration: undefined }} />);

        const toggle = await screen.findByRole("checkbox", { name: "Set duration" });
        await user.click(toggle);
        await user.click(toggle);

        expect(screen.getByText("Form is pristine")).toBeInTheDocument();
    });

    it("reads a typed duration when Enter is pressed", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 3600 }} />);

        const input = await findDurationInput(screen);
        await user.clear(input);
        await user.type(input, "1 hour 30 min");
        await user.keyboard("{Enter}");

        expect(input).toHaveValue("1h 30m");
        expect(screen.getByText("Value: 5400")).toBeInTheDocument();
    });

    it("keeps the last readable duration when the typed text cannot be read", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 3600 }} />);

        const input = await findDurationInput(screen);
        await user.type(input, "x");
        await user.keyboard("{Enter}");

        expect(input).toHaveValue("1h");
        expect(screen.getByText("Value: 3600")).toBeInTheDocument();
    });

    it("picks a duration from the list", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 3600 }} />);

        await user.click(await findDurationInput(screen));
        const dialog = screen.getByRole("dialog", { name: "Choose duration" });
        await user.click(
            within(within(dialog).getByRole("list", { name: "Minutes" })).getByRole("button", { name: "15" })
        );

        expect(await findDurationInput(screen)).toHaveValue("1h 15m");
        expect(screen.getByText("Value: 4500")).toBeInTheDocument();
    });

    it("shows the value again when the form is reset", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 3600 }} />);

        const input = await findDurationInput(screen);
        await user.clear(input);
        await user.type(input, "2h");
        await user.keyboard("{Enter}");
        await user.click(screen.getByRole("button", { name: "Reset" }));

        expect(input).toHaveValue("1h");
        expect(screen.getByText("Value: 3600")).toBeInTheDocument();
    });

    it("clears the value once the text is cleared", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 3600 }} />);

        await user.clear(await findDurationInput(screen));
        await user.keyboard("{Enter}");

        expect(screen.getByText("Value: null")).toBeInTheDocument();
    });

    it("leaves the picker on Tab and keeps the typed duration", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 3600 }} />);

        const input = await findDurationInput(screen);
        await user.clear(input);
        await user.type(input, "2h");
        await user.tab();

        expect(screen.getByRole("button", { name: "Reset" })).toHaveFocus();
        expect(screen.queryByRole("dialog", { name: "Choose duration" })).not.toBeInTheDocument();
        expect(input).toHaveValue("2h");
        expect(screen.getByText("Value: 7200")).toBeInTheDocument();
    });

    it("picks a duration with the arrow keys", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 3600 }} />);

        const input = await findDurationInput(screen);
        await user.click(input);
        await user.keyboard("{ArrowDown}");
        const dialog = screen.getByRole("dialog", { name: "Choose duration" });
        expect(
            within(within(dialog).getByRole("list", { name: "Days" })).getByRole("button", { name: "0" })
        ).toHaveFocus();

        await user.keyboard("{ArrowRight}{ArrowDown}{Enter}");

        expect(
            within(within(dialog).getByRole("list", { name: "Hours" })).getByRole("button", { name: "2" })
        ).toHaveFocus();
        expect(screen.getByText("Value: 7200")).toBeInTheDocument();

        await user.keyboard("{Escape}");

        expect(input).toHaveFocus();
        expect(input).toHaveValue("2h");
        expect(screen.queryByRole("dialog", { name: "Choose duration" })).not.toBeInTheDocument();
    });

    it("selects nothing in the lists while the default is used", async () => {
        const { screen, user } = rtlRender(
            <DurationForm defaultValues={{ isSet: true, duration: null }} defaultTotalSeconds={60} />
        );

        await user.click(await findDurationInput(screen));
        const dialog = screen.getByRole("dialog", { name: "Choose duration" });
        expect(within(dialog).queryAllByRole("button", { pressed: true })).toHaveLength(0);

        await user.keyboard("{ArrowDown}{ArrowRight}{ArrowRight}{ArrowDown}{ArrowDown}{Enter}");

        expect(await findDurationInput(screen)).toHaveValue("2m");
        expect(screen.getByText("Value: 120")).toBeInTheDocument();
    });

    it("moves into the lists with one Down press after the lists were closed", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 3600 }} />);

        await user.click(await findDurationInput(screen));
        await user.keyboard("{Enter}{ArrowDown}");

        const dialog = screen.getByRole("dialog", { name: "Choose duration" });
        expect(
            within(within(dialog).getByRole("list", { name: "Days" })).getByRole("button", { name: "0" })
        ).toHaveFocus();
    });

    it("updates the form while a readable duration is typed", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 3600 }} />);

        const input = await findDurationInput(screen);
        await user.clear(input);
        await user.type(input, "2h");

        expect(input).toHaveFocus();
        expect(screen.getByText("Value: 7200")).toBeInTheDocument();
        expect(screen.getByText("Form is dirty")).toBeInTheDocument();
    });

    it("moves into the lists with Up and jumps by five with Page Down", async () => {
        const { screen, user } = rtlRender(<DurationForm defaultValues={{ isSet: true, duration: 3600 }} />);

        await user.click(await findDurationInput(screen));
        await user.keyboard("{Enter}{ArrowUp}{ArrowRight}{PageDown}");

        const dialog = screen.getByRole("dialog", { name: "Choose duration" });
        expect(
            within(within(dialog).getByRole("list", { name: "Hours" })).getByRole("button", { name: "6" })
        ).toHaveFocus();
    });
});
