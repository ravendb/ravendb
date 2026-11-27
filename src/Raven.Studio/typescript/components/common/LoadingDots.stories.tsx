import { Meta, StoryObj } from "@storybook/react-webpack5";
import React from "react";
import Form from "react-bootstrap/Form";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import Select from "components/common/select/Select";
import { LoadingDots } from "./LoadingDots";

export default {
    title: "Bits/LoadingDots",
    component: LoadingDots,
    decorators: [withStorybookContexts, withBootstrap5],
    args: { label: "Loading" },
} satisfies Meta<typeof LoadingDots>;

type Story = StoryObj<typeof LoadingDots>;

export const Default: Story = {};

export const InsideInputNextToLoadingSelect: Story = {
    render: (args) => (
        <div className="vstack gap-2" style={{ maxWidth: 400 }}>
            <div className="position-relative">
                <Form.Control readOnly />
                <LoadingDots {...args} className="position-absolute top-50 end-0 translate-middle-y" />
            </div>
            <Select options={[]} isLoading placeholder="e.g. Northwind" />
        </div>
    ),
};
