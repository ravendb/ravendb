import { ComponentMeta } from "@storybook/react";
import React from "react";
import { ProgressCircle } from "./ProgressCircle";

export default {
    title: "Bits/Progress circle",
    component: ProgressCircle,
} as ComponentMeta<typeof ProgressCircle>;

const Template = () => {
    return (
        <div>
            <h3>Regular:</h3>

            <ProgressCircle state="success" icon="icon-check">
                OK
            </ProgressCircle>

            <ProgressCircle state="running" icon="icon-pause" progress={0.75}>
                Paused
            </ProgressCircle>

            <ProgressCircle state="running" progress={0.75}>
                Running
            </ProgressCircle>

            <ProgressCircle state="running">Running</ProgressCircle>

            <ProgressCircle state="failed" icon="icon-cancel">
                Error
            </ProgressCircle>

            <h3>Inline</h3>

            <ProgressCircle state="success" icon="icon-check" inline>
                OK
            </ProgressCircle>

            <ProgressCircle state="running" icon="icon-pause" progress={0.75} inline>
                Paused
            </ProgressCircle>

            <ProgressCircle state="running" progress={0.75} inline>
                Running
            </ProgressCircle>

            <ProgressCircle state="running" inline>
                Running
            </ProgressCircle>

            <ProgressCircle state="failed" icon="icon-cancel" inline>
                Error
            </ProgressCircle>
        </div>
    );
};

export const States = Template.bind({});
