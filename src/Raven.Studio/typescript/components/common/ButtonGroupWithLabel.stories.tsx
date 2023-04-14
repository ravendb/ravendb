import { ComponentMeta } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Button } from "reactstrap";
import { ButtonGroupWithLabel } from "./ButtonGroupWithLabel";
import { Icon } from "./Icon";

export default {
    title: "Bits/Buttons",
    decorators: [withStorybookContexts, withBootstrap5],
    component: Button,
} as ComponentMeta<typeof Button>;

export function GroupWithLabel() {
    return (
        <>
            <ButtonGroupWithLabel label="Button group with label">
                <Button color="danger">
                    <Icon icon="trash" />
                    <span>Delete</span>
                </Button>
                <Button>
                    <Icon icon="lock" />
                    <span>Set delete lock mode</span>
                </Button>
            </ButtonGroupWithLabel>
        </>
    );
}
