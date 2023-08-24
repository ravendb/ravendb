import { Meta } from "@storybook/react";
import React, { useState } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import DurationPicker from "./DurationPicker";
import genUtils from "common/generalUtils";

export default {
    title: "Bits/DurationPicker",
    component: DurationPicker,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DurationPicker>;

export function WithAllOptions() {
    const [totalSeconds, setTotalSeconds] = useState(123654);

    return (
        <div className="p4">
            <DurationPicker totalSeconds={totalSeconds} onChange={setTotalSeconds} showDays showSeconds />
            Formatted: {genUtils.formatMillis(totalSeconds * 1000)}
        </div>
    );
}
