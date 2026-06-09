import type { WizardSteps } from "@/components/form/wizard/form-wizard";
import type { AgentFormData, AgentStepId } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { ChooseCapabilityStep } from "@/pages/setup/add-capability-wizard/steps/capability/choose-capability-step";
import { ConnectProviderStep } from "@/pages/setup/add-capability-wizard/steps/connect/connect-provider-step";
import { useConnectProviderStep } from "@/pages/setup/add-capability-wizard/steps/connect/use-connect-provider-step";
import { CreateAgentStep } from "@/pages/setup/add-capability-wizard/steps/create/create-agent-step";
import { ReviewAgentStep } from "@/pages/setup/add-capability-wizard/steps/review/review-agent-step";
import { BindChannelsStep } from "@/pages/setup/add-capability-wizard/steps/channels/bind-channels-step";

export const CAPABILITY_FLOW: AgentStepId[] = ["capability", "connection", "create", "review", "channels"];

export const useCapabilitySteps = (): WizardSteps<AgentStepId, AgentFormData> => {
    const connectProviderStep = useConnectProviderStep();

    return {
        capability: {
            title: "Choose an AI Capability",
            bodyComponent: ChooseCapabilityStep,
            validate: "capability",
        },
        connection: {
            title: "Connect your agent to AI Provider",
            description: "Choose the AI provider connection string your agent will use.",
            bodyComponent: ConnectProviderStep,
            validate: "connection",
            beforeNext: connectProviderStep.mutateAsync,
            isPending: connectProviderStep.isPending,
            error: connectProviderStep.error,
        },
        create: {
            title: "Create your Agent with AI",
            description: "We analyzed your collections and propose a few framings. Pick one to edit.",
            bodyComponent: CreateAgentStep,
            validate: "create",
        },
        review: {
            title: "Review & edit your agent",
            bodyComponent: ReviewAgentStep,
            validate: "review",
        },
        channels: {
            title: "Bind your agent to channels",
            bodyComponent: BindChannelsStep,
            validate: false,
        },
    };
};
