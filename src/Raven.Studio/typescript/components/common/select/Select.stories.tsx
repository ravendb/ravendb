import { Meta } from "@storybook/react";
import Select, { SelectOptionWithIconAndSeparator } from "./Select";
import SelectCreatable from "./SelectCreatable";
import React from "react";
import { Col, Label } from "reactstrap";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";

export default {
    title: "Bits/Selects",
    component: Select,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof Select>;

const options: SelectOptionWithIconAndSeparator<number>[] = [
    { label: "Option 1", value: 11, icon: "orchestrator", iconColor: "orchestrator" },
    { label: "Option 2", value: 22, icon: "disable", iconColor: "danger", horizontalSeparatorLine: true },
    { label: "Option 3", value: 33 },
];

export function Selects() {
    return (
        <Col md={4} className="vstack gap-3">
            <div>
                <Label>Normal</Label>
                <Select options={options} />
            </div>
            <div>
                <Label>Disabled</Label>
                <Select options={options} isDisabled />
            </div>
            <div>
                <Label>Multi</Label>
                <Select options={options} isMulti />
            </div>
            <div>
                <Label>Creatable</Label>
                <SelectCreatable options={options} />
            </div>
        </Col>
    );
}
