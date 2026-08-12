import type { WizardBadgeContext, WizardSteps } from "@/components/form/wizard/form-wizard";
import type { AgentFormData, AgentStepId } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { ChooseCapabilityStep } from "@/pages/setup/add-capability-wizard/steps/capability/choose-capability-step";
import { ConnectProviderStep } from "@/pages/setup/add-capability-wizard/steps/connect/connect-provider-step";
import { CreateAgentStep } from "@/pages/setup/add-capability-wizard/steps/create/create-agent-step";
import { useCreateAgentStep } from "@/pages/setup/add-capability-wizard/steps/create/use-create-agent-step";
import { useIsSuggestingAgents } from "@/pages/setup/add-capability-wizard/steps/create/use-suggested-agents";
import { ReviewAgentStep } from "@/pages/setup/add-capability-wizard/steps/review/review-agent-step";
import { ReviewTestAgentButton } from "@/pages/setup/add-capability-wizard/steps/review/test-agent-sheet";
import { useProvisionAgentStep } from "@/pages/setup/add-capability-wizard/steps/review/use-provision-agent-step";
import { ChannelsStep } from "@/pages/setup/add-capability-wizard/steps/channels/channels-step";
import { CAPABILITY_OPTIONS } from "@/pages/setup/add-capability-wizard/steps/capability/capability-options";
import { Badge } from "@/components/shadcn/ui/badge";
import { getOptionLabel } from "@/lib/form-utils";

export const CAPABILITY_FLOW: AgentStepId[] = ["capability", "connection", "create", "review", "channels"];

export const useCapabilitySteps = (): WizardSteps<AgentStepId, AgentFormData> => {
    const createAgentBeforeNext = useCreateAgentStep();
    const provisionAgentBeforeNext = useProvisionAgentStep();
    const isSuggestingAgents = useIsSuggestingAgents();

    return {
        capability: {
            title: "Choose an AI Capability",
            bodyComponent: ChooseCapabilityStep,
            validate: "capability",
            badgeFields: ["capability.type"],
            badge: ({ isComplete, values }: WizardBadgeContext<AgentFormData>) => {
                if (!isComplete) {
                    return null;
                }
                return <Badge variant="primary">{getOptionLabel(CAPABILITY_OPTIONS, values.capability.type)}</Badge>;
            },
        },
        connection: {
            title: "Connect your agent to AI Provider",
            description: "Choose the AI provider connection string your agent will use.",
            bodyComponent: ConnectProviderStep,
            validate: "connection",
            // Only the AI Agent capability exists today, so there's nothing else to pick on the
            // previous step; hide Back so users can't land on it and get stuck.
            canGoBack: false,
            badge: ({ isComplete }: WizardBadgeContext<AgentFormData>) => {
                if (!isComplete) {
                    return null;
                }
                return <Badge variant="success">Successfully connected</Badge>;
            },
        },
        create: {
            title: "Create your Agent with AI",
            description:
                "We analyzed your collections and propose a few agents. Pick one, describe your own for AI to generate, or set up manually.",
            bodyComponent: CreateAgentStep,
            validate: "create",
            beforeNext: createAgentBeforeNext,
            isNextDisabled: isSuggestingAgents,
        },
        review: {
            title: "Review & edit your agent",
            bodyComponent: ReviewAgentStep,
            validate: "review",
            // Provisions the agent on the way out, then the wizard advances to the optional
            // channels step.
            beforeNext: provisionAgentBeforeNext,
            nextLabel: "Save agent",
            footerComponent: ReviewTestAgentButton,
            badge: ({ isComplete }: WizardBadgeContext<AgentFormData>) => {
                if (!isComplete) {
                    return null;
                }
                return <Badge variant="success">Agent created</Badge>;
            },
        },
        channels: {
            title: "Add a channel",
            description:
                "Your agent is ready. This step is optional — connect a channel so people can reach it, or skip for now and add channels later.",
            bodyComponent: ChannelsStep,
            // Channels are created through their own API, not via the wizard form.
            validate: false,
            // The agent was already committed on the review step. Do not offer navigation that
            // could provision it a second time or imply that cancelling would undo it.
            canCancel: false,
            canGoBack: false,
        },
    };
};
