import type { Meta, StoryObj } from "@storybook/react-vite";
import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { FormProvider, useForm } from "react-hook-form";
import { FormWizard } from "@/components/form/wizard/form-wizard";
import { preventEnterKeySubmission } from "@/lib/form-utils";
import { sampleAgentSuggestion } from "@/mocks/apps-mocks";
import { AddCapabilityWizard } from "./add-capability-wizard";
import { suggestionToAgentConfiguration } from "./agent-config-form";
import { CAPABILITY_FLOW, useCapabilitySteps } from "./capability-wizard-flow";
import { useCapabilityWizardStore } from "./capability-wizard-store";
import { agentSchema, type AgentFormData, type AgentStepId } from "./capability-wizard-validation";

const meta = {
    title: "Setup/Add Capability Wizard",
    component: AddCapabilityWizard,
    parameters: {
        page: { bare: true },
        // "Add agent" links here with ?capability=agent, which skips the capability step.
        router: {
            initialPath: "/apps/demo/capability/add?capability=agent",
            path: "/apps/:slug/capability/add",
        },
    },
} satisfies Meta<typeof AddCapabilityWizard>;

export default meta;

type Story = StoryObj<typeof meta>;

// The full wizard through the real entry component (form creation, store reset, and the
// provision mutation wired up). Starts on the connection step because of ?capability=agent.
export const Default: Story = {};

// Valid values for every step, so any step renders with realistic data and Next/Back keep
// working against the default MSW mocks. Seeded from the same suggestion the mocks return.
const SEED: AgentFormData = {
    capability: { type: "agent" },
    connection: { connectionStringName: "openai-chat" },
    create: { mode: "ai", selectedIndex: 0 },
    review: suggestionToAgentConfiguration(sampleAgentSuggestion.configurations[0]),
};

// Renders the real wizard jumped to a single step. The create/review steps read the AI
// suggestions from the store, so seed them before those steps first render (Default's
// AddCapabilityWizard resets the store on mount, so this never leaks).
function CapabilityWizardAtStep({ initialStep }: { initialStep: AgentStepId }) {
    useState(() => useCapabilityWizardStore.setState({ suggestions: sampleAgentSuggestion.configurations }));

    const form = useForm<AgentFormData>({
        mode: "onChange",
        defaultValues: SEED,
        resolver: zodResolver(agentSchema),
    });

    return (
        <FormProvider {...form}>
            <form className="h-full" onSubmit={(event) => event.preventDefault()} onKeyDown={preventEnterKeySubmission}>
                <CapabilityWizardStepBody initialStep={initialStep} />
            </form>
        </FormProvider>
    );
}

// useCapabilitySteps reads the form via context, so it must run inside the provider above.
function CapabilityWizardStepBody({ initialStep }: { initialStep: AgentStepId }) {
    const steps = useCapabilitySteps();

    return (
        <FormWizard
            steps={steps}
            flow={CAPABILITY_FLOW}
            initialStep={initialStep}
            cancel={() => {}}
            submitLabel="Save agent"
        />
    );
}

export const ChooseCapability: Story = {
    render: () => <CapabilityWizardAtStep initialStep="capability" />,
};

export const ConnectProvider: Story = {
    render: () => <CapabilityWizardAtStep initialStep="connection" />,
};

export const CreateAgent: Story = {
    render: () => <CapabilityWizardAtStep initialStep="create" />,
};

export const ReviewAgent: Story = {
    render: () => <CapabilityWizardAtStep initialStep="review" />,
};
