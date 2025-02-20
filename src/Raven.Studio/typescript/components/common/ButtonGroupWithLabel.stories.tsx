import { Meta } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { ButtonGroupWithLabel } from "./ButtonGroupWithLabel";
import { Icon } from "./Icon";
import Button from "react-bootstrap/Button";

export default {
    title: "Bits/Buttons",
    decorators: [withStorybookContexts, withBootstrap5],
    component: Button,
} satisfies Meta<typeof Button>;

export function GroupWithLabel() {
    return (
        <>
            <ButtonGroupWithLabel label="Button group with label">
                <Button variant="danger">
                    <Icon icon="trash" />
                    <span>Delete</span>
                </Button>
                <Button variant="secondary">
                    <Icon icon="lock" />
                    <span>Set delete lock mode</span>
                </Button>
            </ButtonGroupWithLabel>
        </>
    );
}
