import { ComponentMeta } from "@storybook/react";
import { CheckboxMultiToggle, RadioMultiToggle, RadioToggle } from "./MultiToggle";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Card } from "reactstrap";

export default {
    title: "Bits/Multitoggle",
    component: RadioMultiToggle,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof RadioMultiToggle>;

export function MultiToggles() {
    return (
        <Card className="p-2">
            <div className="p-2">
                <RadioToggle></RadioToggle>
                <RadioMultiToggle className="ms-4"></RadioMultiToggle>
                <CheckboxMultiToggle className="ms-4"></CheckboxMultiToggle>
            </div>
        </Card>
    );
}
