import { Meta } from "@storybook/react-webpack5";
import React, { useState } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import DurationPicker, { DurationPickerProps } from "./DurationPicker";

export default {
    title: "Bits/DurationPicker",
    component: DurationPicker,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DurationPicker>;

type PickerVariantProps = Pick<DurationPickerProps, "showSeconds" | "defaultTotalSeconds">;

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
            <DurationPicker totalSeconds={totalSeconds} onChange={setTotalSeconds} {...variantProps} />
            <small className="text-muted">{`Saved value: ${totalSeconds ?? "empty"} seconds`}</small>
        </div>
    );
}

export function AllUnitSets() {
    return (
        <div className="p-4" style={{ minHeight: 420 }}>
            <PickerPreview title="Days, Hours, Minutes (retention fields)" initialTotalSeconds={100800} />
            <PickerPreview
                title="Days, Hours, Minutes, Seconds (frequency fields, Document Revisions)"
                initialTotalSeconds={123654}
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
