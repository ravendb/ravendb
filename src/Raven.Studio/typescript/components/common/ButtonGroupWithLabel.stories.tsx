import { ComponentMeta } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Button } from "reactstrap";
import { ButtonGroupWithLabel } from "./ButtonGroupWithLabel";

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
                    <i className="icon-trash"></i>
                    <span>Delete</span>
                </Button>
                <Button>
                    <i className="icon-lock"></i>
                    <span>Set delete lock mode</span>
                </Button>
            </ButtonGroupWithLabel>
        </>
    );
}
