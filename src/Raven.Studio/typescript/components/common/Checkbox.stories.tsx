import { ComponentMeta } from "@storybook/react";
import { Checkbox, Radio, Switch } from "./Checkbox";
import useBoolean from "hooks/useBoolean";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { boundCopy } from "../utils/common";

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
            <br />
            <Checkbox selected={selected} toggleSelection={toggle} color="primary">
                Primary checkbox
            </Checkbox>
            <br />

            <Checkbox selected={selected} toggleSelection={toggle} color="success" size="lg">
                Checkbox lg
            </Checkbox>
            <br />
            <Checkbox selected={selected} toggleSelection={toggle} color="success" size="sm">
                Checkbox sm ¯\_(ツ)_/¯
            </Checkbox>
            <br />
            <Checkbox selected={selected} toggleSelection={toggle} color="danger" size="lg" reverse>
                Checkbox reverse
            </Checkbox>
            <br />
            <Switch selected={selected} toggleSelection={toggle}>
                Switch
            </Switch>
            <br />
            <Switch size="lg" selected={selected} toggleSelection={toggle} color="primary">
                Large primary switch
            </Switch>
            <br />
            <Switch selected={selected} toggleSelection={toggle} color="info">
                Info switch
            </Switch>
            <br />
            <Switch size="sm" selected={selected} toggleSelection={toggle} color="node">
                tiny winy switch
            </Switch>
            <br />
            <Switch selected={selected} toggleSelection={toggle} reverse color="warning">
                Switch reverse
            </Switch>

            <Radio selected={selected} toggleSelection={toggle}>
                Radio
            </Radio>
            <br />
            <Radio selected={selected} toggleSelection={toggle} color="primary" size="sm">
                Radio sm
            </Radio>
            <br />
            <Radio selected={selected} toggleSelection={toggle} color="node" size="lg">
                Radio lg
            </Radio>
            <br />
            <Radio selected={selected} toggleSelection={toggle} color="shard" reverse>
                Radio reverse
            </Radio>
            <Radio selected={selected} toggleSelection={toggle} color="danger" disabled>
                Radio disabled
            </Radio>
        </div>
    );
};

export const WithLabel = boundCopy(Template);

WithLabel.args = {
    withLabel: true,
};
