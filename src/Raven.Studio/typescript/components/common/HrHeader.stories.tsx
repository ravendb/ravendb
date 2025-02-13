import { Meta } from "@storybook/react";
import { HrHeader } from "./HrHeader";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/HrHeader",
    component: HrHeader,
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=8-7701",
        },
    },
} satisfies Meta<typeof HrHeader>;

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
