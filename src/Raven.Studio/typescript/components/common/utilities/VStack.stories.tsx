import React from "react";
import Card from "react-bootstrap/Card";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { VStack, VStackProps } from "components/common/utilities/VStack";

export default {
    title: "Utilities/VStack",
    component: VStack,
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        gap: {
            control: { type: "object" },
            description: "Spacing between items. Accepts a number or a breakpoint-based object.",
            table: {
                type: { summary: "number | object" },
                defaultValue: { summary: 3 },
            },
        },
        className: {
            control: { type: "text" },
            description: "Additional CSS classes for the VStack container.",
            table: {
                type: { summary: "string" },
                defaultValue: { summary: "undefined" },
            },
        },
    },
};

const Template = (args: VStackProps) => (
    <VStack {...args}>
        <Card>
            <Card.Body>
                <p>Card 1 content</p>
            </Card.Body>
        </Card>
        <Card>
            <Card.Body>
                <p>Card 2 content</p>
            </Card.Body>
        </Card>
        <Card>
            <Card.Body>
                <p>Card 3 content</p>
            </Card.Body>
        </Card>
    </VStack>
);

export const Default = Template.bind({});
Default.args = {
    gap: 3,
    className: "",
};

export const ResponsiveGap = Template.bind({});
ResponsiveGap.args = {
    gap: { sm: 3, md: 4, lg: 5, xl: 6, xxl: 7 },
    className: "",
};
