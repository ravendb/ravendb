// import { Meta } from "@storybook/react";
import { HrHeader } from "./HrHeader";
import React from "react";
// import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/HrHeaderWitam",
    // component: HrHeader,
    // decorators: [withStorybookContexts, withBootstrap5],
};

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
