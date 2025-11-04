import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Sheet, SheetClose, SheetContent, SheetTrigger } from "components/common/Sheet";
import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import Button from "react-bootstrap/Button";

export default {
    title: "Bits/Sheet",
    decorators: [withStorybookContexts, withBootstrap5],
    component: Sheet,
} satisfies Meta;

export const Template: StoryObj = {
    render: () => {
        return (
            <Sheet>
                <SheetTrigger>
                    <Button>Open Sheet</Button>
                </SheetTrigger>

                <SheetContent>
                    <div>
                        <h2>My Sheet</h2>
                        <SheetClose>
                            <Button>Close</Button>
                        </SheetClose>
                    </div>
                </SheetContent>
            </Sheet>
        );
    },
};
