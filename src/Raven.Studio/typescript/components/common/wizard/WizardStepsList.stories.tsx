import { useState } from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import WizardStepsList, { WizardStepsListItem } from "./WizardStepsList";

type StepId = "type" | "settings" | "schedule" | "destinations" | "summary";

interface WizardStepsListStoryArgs {
    currentStepId: StepId;
    hasLongValues: boolean;
    containerWidth: number;
}

const STEPS: WizardStepsListItem<StepId>[] = [
    { id: "type", title: "Select backup type" },
    { id: "settings", title: "Configure settings" },
    { id: "schedule", title: "Schedule backups" },
    { id: "destinations", title: "Define destinations" },
    { id: "summary", title: "Review task configuration" },
];

const LONG_TITLE_SUFFIX = " for the production orders database in the European region";

function WizardStepsListStory({ currentStepId, hasLongValues, containerWidth }: WizardStepsListStoryArgs) {
    const [currentStep, setCurrentStep] = useState(currentStepId);

    const steps: WizardStepsListItem<StepId>[] = hasLongValues
        ? STEPS.map((x) => ({ ...x, title: x.title + LONG_TITLE_SUFFIX }))
        : STEPS;

    return (
        <div className="panel-bg-1 p-4" style={{ width: containerWidth }}>
            <WizardStepsList<StepId> steps={steps} currentStepId={currentStep} onStepClick={setCurrentStep} />
        </div>
    );
}

export default {
    title: "Bits/Wizard/WizardStepsList",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    argTypes: {
        currentStepId: { control: "select", options: STEPS.map((x) => x.id) },
    },
    args: {
        currentStepId: "schedule",
        hasLongValues: false,
        containerWidth: 380,
    },
    render: (args) => <WizardStepsListStory {...args} />,
} satisfies Meta<WizardStepsListStoryArgs>;

type Story = StoryObj<WizardStepsListStoryArgs>;

export const Default: Story = {};

export const FirstStep: Story = {
    args: { currentStepId: "type" },
};

export const LastStep: Story = {
    args: { currentStepId: "summary" },
};

export const LongValues: Story = {
    args: { hasLongValues: true, containerWidth: 260 },
};
