import { ComponentMeta } from "@storybook/react";
import { HrHeader } from "./HrHeader";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/HrHeader",
    component: HrHeader,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof HrHeader>;

export function HrHeaders() {
    return (
        <div>
            <HrHeader>Header with divider</HrHeader>

            <HrHeader
                right={
                    <>
                        <strong>Additional content</strong>
                    </>
                }
            >
                Header with divider
            </HrHeader>
        </div>
    );
}
