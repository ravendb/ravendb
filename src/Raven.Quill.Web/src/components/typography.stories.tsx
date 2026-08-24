import type { Meta, StoryObj } from "@storybook/react-vite";
import { Heading, Text } from "./typography";

const meta = {
    title: "Foundations/Typography",
} satisfies Meta;

export default meta;

type Story = StoryObj;

export const Headings: Story = {
    render: () => (
        <div className="space-y-3">
            <Heading as="h1" variant="page">
                Page title (variant="page")
            </Heading>
            <Heading as="h2" variant="title">
                Title (variant="title")
            </Heading>
            <Heading as="h2" variant="section">
                Section (variant="section")
            </Heading>
            <Heading as="h3" variant="subsection">
                Subsection (variant="subsection")
            </Heading>
            <Heading as="h4" variant="label">
                Label (variant="label")
            </Heading>
        </div>
    ),
};

export const Texts: Story = {
    render: () => (
        <div className="space-y-2">
            <Text variant="body">Body — the default paragraph text.</Text>
            <Text variant="muted">Muted — secondary supporting copy.</Text>
            <Text variant="caption">Caption — small metadata and hints.</Text>
            <Text variant="label">Label — emphasized inline label.</Text>
        </div>
    ),
};
