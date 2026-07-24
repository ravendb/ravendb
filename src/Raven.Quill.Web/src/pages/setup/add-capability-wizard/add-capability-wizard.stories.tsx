import type { Meta, StoryObj } from "@storybook/react-vite";
import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { FormProvider, useForm } from "react-hook-form";
import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { FormWizard } from "@/components/form/wizard/form-wizard";
import { preventEnterKeySubmission } from "@/lib/form-utils";
import { appsMocks, sampleAgentSuggestion } from "@/mocks/apps-mocks";
import { channelsMocks } from "@/mocks/channels-mocks";
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
    create: { mode: "ai", selectedIndex: 0, promptInput: "" },
    review: suggestionToAgentConfiguration(sampleAgentSuggestion.configurations[0]),
};

// Renders the real wizard jumped to a single step. The create/review steps read the AI
// suggestions from the store, so seed them before those steps first render (Default's
// AddCapabilityWizard resets the store on mount, so this never leaks).
function CapabilityWizardAtStep({ initialStep }: { initialStep: AgentStepId }) {
    // Seed the store so any step renders with realistic data. The channels step is entered
    // directly (no provisioning runs first), so seed its created agent; other steps provision
    // on the way in and must start without one so "Save agent" actually creates the agent.
    useState(() =>
        useCapabilityWizardStore.setState({
            suggestions: sampleAgentSuggestion.configurations,
            createdAgent: initialStep === "channels" ? { agentId: "agents/sales", name: "Sales assistant" } : null,
        }),
    );

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
    const { slug = "" } = useParams();
    const steps = useCapabilitySteps();
    const createdAgent = useCapabilityWizardStore((state) => state.createdAgent);

    // Mirror the real component: the optional channels step reads "Skip for now" until a channel
    // is connected. Lets the Channels / ChannelsEmpty stories show each label.
    const channelsQuery = useQuery({
        ...api.queries.channels.list(slug),
        enabled: Boolean(createdAgent),
    });
    const hasChannels =
        createdAgent != null && (channelsQuery.data ?? []).some((channel) => channel.agentId === createdAgent.agentId);

    return (
        <FormWizard
            steps={steps}
            flow={CAPABILITY_FLOW}
            initialStep={initialStep}
            cancel={() => {}}
            completion={{ type: "action", label: hasChannels ? "Finish" : "Skip for now", onComplete: () => {} }}
        />
    );
}

export const ChooseCapability: Story = {
    render: () => <CapabilityWizardAtStep initialStep="capability" />,
};

export const ConnectProvider: Story = {
    render: () => <CapabilityWizardAtStep initialStep="connection" />,
};

// No connection strings yet: the step hides the selector and shows only the "Add" button.
// Overriding the `apps` key drops its other default handlers, so re-add the suggestion
// mock the step prefetches.
export const ConnectProviderEmpty: Story = {
    render: () => <CapabilityWizardAtStep initialStep="connection" />,
    parameters: {
        msw: { handlers: { apps: [appsMocks.aiConnectionStringsList([]), appsMocks.suggestAgent()] } },
    },
};

export const CreateAgent: Story = {
    render: () => <CapabilityWizardAtStep initialStep="create" />,
};

export const ReviewAgent: Story = {
    render: () => <CapabilityWizardAtStep initialStep="review" />,
};

export const Channels: Story = {
    render: () => <CapabilityWizardAtStep initialStep="channels" />,
};

export const ChannelsEmpty: Story = {
    render: () => <CapabilityWizardAtStep initialStep="channels" />,
    parameters: { msw: { handlers: { channels: [channelsMocks.list([])] } } },
};
