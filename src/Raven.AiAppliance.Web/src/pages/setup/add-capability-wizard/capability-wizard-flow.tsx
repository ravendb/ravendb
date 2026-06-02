import type { WizardSteps } from "@/components/form/wizard/form-wizard";
import type { AgentStepId } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { ChooseCapabilityStep } from "@/pages/setup/add-capability-wizard/steps/capability/choose-capability-step";
import { ConnectProviderStep } from "@/pages/setup/add-capability-wizard/steps/connect/connect-provider-step";
import { useConnectProviderStep } from "@/pages/setup/add-capability-wizard/steps/connect/use-connect-provider-step";
import { CreateAgentStep } from "@/pages/setup/add-capability-wizard/steps/create/create-agent-step";
import { ReviewAgentStep } from "@/pages/setup/add-capability-wizard/steps/review/review-agent-step";
import {
    BindChannelsFooter,
    BindChannelsStep,
} from "@/pages/setup/add-capability-wizard/steps/channels/bind-channels-step";

export const CAPABILITY_FLOW: AgentStepId[] = ["capability", "connection", "create", "review", "channels"];

export const useCapabilitySteps = (): WizardSteps<AgentStepId> => {
    const connectProviderStep = useConnectProviderStep();

    return {
        capability: {
            id: "capability",
            title: "Choose an AI Capability",
            bodyComponent: ChooseCapabilityStep,
        },
        connection: {
            id: "connection",
            title: "Connect your agent to AI Provider",
            description: "Choose the AI provider connection string your agent will use.",
            bodyComponent: ConnectProviderStep,
            beforeNext: connectProviderStep.mutateAsync,
            status: connectProviderStep.status,
            error: connectProviderStep.error,
        },
        create: {
            id: "create",
            title: "Create your Agent with AI",
            description: "We analyzed your collections and propose a few framings. Pick one to edit.",
            bodyComponent: CreateAgentStep,
        },
        review: {
            id: "review",
            title: "Review & edit your agent",
            bodyComponent: ReviewAgentStep,
        },
        channels: {
            id: "channels",
            title: "Bind your agent to channels",
            bodyComponent: BindChannelsStep,
            footerComponent: BindChannelsFooter,
            skipValidation: true,
        },
    };
};
