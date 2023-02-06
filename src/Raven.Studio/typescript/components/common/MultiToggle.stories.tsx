import { ComponentMeta } from "@storybook/react";
import { MultiToggle, RadioToggle } from "./MultiToggle";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Card } from "reactstrap";

export default {
    title: "Bits/Multitoggle",
    component: MultiToggle,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof MultiToggle>;

export function MultiToggles() {
    const radioList = [
        { value: "1hour", label: "1 Hour" },
        { value: "6hours", label: "6 hours" },
        { value: "12hours", label: "12 hours" },
        { value: "1day", label: "1 day" },
    ];

    const checkboxList = [
        { value: "normal", label: "Normal" },
        { value: "error", label: "Error/Faulty" },
        { value: "stale", label: "Stale" },
        { value: "rolling", label: "Rolling deployment" },
        { value: "paused", label: "Paused" },
        { value: "disabled", label: "Disabled" },
        { value: "idle", label: "Idle" },
    ];
    return (
        <Card className="p-2">
            <div className="p-2 d-flex flex-wrap">
                <RadioToggle></RadioToggle>

                <MultiToggle radio inputList={radioList} label="Auto backup frequency" className="ms-4"></MultiToggle>

                <MultiToggle inputList={checkboxList} label="Filter indexes" className="ms-4"></MultiToggle>
            </div>
        </Card>
    );
}
