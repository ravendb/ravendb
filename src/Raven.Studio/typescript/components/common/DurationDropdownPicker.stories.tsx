import { Meta } from "@storybook/react-webpack5";
import React, { useState } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import DurationDropdownPicker, { DurationPickerProps } from "./DurationDropdownPicker";

export default {
    title: "Bits/DurationDropdownPicker",
    component: DurationDropdownPicker,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DurationDropdownPicker>;

type PickerVariantProps = Pick<DurationPickerProps, "showDays" | "showSeconds" | "defaultTotalSeconds">;

function PickerPreview({
    title,
    initialTotalSeconds,
    ...variantProps
}: PickerVariantProps & {
    title: string;
    initialTotalSeconds: number;
}) {
    const [totalSeconds, setTotalSeconds] = useState(initialTotalSeconds);

    return (
        <div className="mb-4" style={{ maxWidth: 360 }}>
            <div className="mb-1">{title}</div>
            <DurationDropdownPicker totalSeconds={totalSeconds} onChange={setTotalSeconds} {...variantProps} />
            <small className="text-muted">{`Saved value: ${totalSeconds ?? "empty"} seconds`}</small>
        </div>
    );
}

export function AllUnitSets() {
    return (
        <div className="p-4" style={{ minHeight: 420 }}>
            <PickerPreview title="Hours, Minutes" initialTotalSeconds={5400} />
            <PickerPreview title="Hours, Minutes, Seconds (frequency fields)" initialTotalSeconds={60} showSeconds />
            <PickerPreview title="Days, Hours, Minutes (retention fields)" initialTotalSeconds={100800} showDays />
            <PickerPreview
                title="Days, Hours, Minutes, Seconds (Document Revisions)"
                initialTotalSeconds={123654}
                showDays
                showSeconds
            />
        </div>
    );
}

export function WithDefaultValue() {
    return (
        <div className="p-4" style={{ minHeight: 320 }}>
            <PickerPreview
                title="Refresh frequency (empty, default 1 minute)"
                initialTotalSeconds={null}
                defaultTotalSeconds={60}
                showSeconds
            />
        </div>
    );
}
