import Button from "react-bootstrap/Button";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import WizardLayout from "./WizardLayout";
import WizardStepsList, { WizardStepsListItem } from "./WizardStepsList";

interface WizardLayoutStoryArgs {
    hasLongContent: boolean;
    hasLongValues: boolean;
}

type StepId = "type" | "settings" | "summary";

const STEPS: WizardStepsListItem<StepId>[] = [
    { id: "type", title: "Select backup type" },
    { id: "settings", title: "Configure settings" },
    { id: "summary", title: "Review task configuration" },
];

const PARAGRAPH =
    "Customize your backup parameters or proceed with the recommended default settings. " +
    "Balance storage efficiency against restoration speed.";

const UNBREAKABLE_VALUE =
    "/var/lib/ravendb/backups/production/europe/orders/nightly/full-and-incremental-backups-folder-with-a-very-long-name";

function WizardLayoutStory({ hasLongContent, hasLongValues }: WizardLayoutStoryArgs) {
    const paragraphsCount = hasLongContent ? 40 : 2;

    return (
        <div style={{ height: "90vh" }}>
            <WizardLayout
                onSubmit={(event) => event.preventDefault()}
                content={
                    <div>
                        <h2>Configure settings</h2>
                        {hasLongValues && <p className="text-break">{UNBREAKABLE_VALUE.repeat(3)}</p>}
                        {Array.from({ length: paragraphsCount }, (_, idx) => (
                            <p key={idx}>{PARAGRAPH}</p>
                        ))}
                    </div>
                }
                footer={
                    <div className="hstack gap-2 justify-content-between">
                        <Button variant="outline-secondary" className="rounded-pill">
                            Cancel
                        </Button>
                        <Button variant="primary" className="rounded-pill">
                            Next
                        </Button>
                    </div>
                }
                sidebar={<WizardStepsList steps={STEPS} currentStepId="settings" onStepClick={() => null} />}
            />
        </div>
    );
}

export default {
    title: "Bits/Wizard/WizardLayout",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    args: {
        hasLongContent: false,
        hasLongValues: false,
    },
    render: (args) => <WizardLayoutStory {...args} />,
} satisfies Meta<WizardLayoutStoryArgs>;

type Story = StoryObj<WizardLayoutStoryArgs>;

export const Default: Story = {};

export const LongContent: Story = {
    args: { hasLongContent: true },
};

export const LongValues: Story = {
    args: { hasLongValues: true },
};
