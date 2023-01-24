import { ComponentMeta } from "@storybook/react";
import { Checkbox, Switch } from "./Checkbox";
import useBoolean from "hooks/useBoolean";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { boundCopy } from "../utils/common";
import { Input } from "reactstrap";

export default {
    title: "Bits/Checkbox",
    decorators: [withStorybookContexts, withBootstrap5],
    component: Checkbox,
} as ComponentMeta<typeof Checkbox>;

const Template = (args: { withLabel: boolean }) => {
    const { value: selected, toggle } = useBoolean(false);

    return (
        <div>
            <Checkbox selected={selected} toggleSelection={toggle}>
                First checkbox
            </Checkbox>
            <Checkbox selected={selected} toggleSelection={toggle} color="primary">
                Primary checkbox
            </Checkbox>
            {/* TODO fix vertical aligment */}
            <Checkbox selected={selected} toggleSelection={toggle} color="success" size="lg">
                Checkbox lg
            </Checkbox>
            <Checkbox selected={selected} toggleSelection={toggle} color="success" size="sm">
                Checkbox sm ¯\_(ツ)_/¯
            </Checkbox>
            <Checkbox selected={selected} toggleSelection={toggle} color="danger" size="lg" reverse>
                Checkbox reverse
            </Checkbox>
            <Switch selected={selected} toggleSelection={toggle}>
                Switch
            </Switch>
            <Switch selected={selected} toggleSelection={toggle} color="info">
                Info switch
            </Switch>
            <Switch selected={selected} toggleSelection={toggle} reverse color="warning">
                Switch reverse
            </Switch>
        </div>
    );
};

export const WithLabel = boundCopy(Template);

WithLabel.args = {
    withLabel: true,
};
