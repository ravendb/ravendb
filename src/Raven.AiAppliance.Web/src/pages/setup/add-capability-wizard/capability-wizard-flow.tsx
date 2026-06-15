import type { WizardBadgeContext, WizardSteps } from "@/components/form/wizard/form-wizard";
import type { AgentFormData, AgentStepId } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { ChooseCapabilityStep } from "@/pages/setup/add-capability-wizard/steps/capability/choose-capability-step";
import { ConnectProviderStep } from "@/pages/setup/add-capability-wizard/steps/connect/connect-provider-step";
import { useConnectProviderStep } from "@/pages/setup/add-capability-wizard/steps/connect/use-connect-provider-step";
import { CreateAgentStep } from "@/pages/setup/add-capability-wizard/steps/create/create-agent-step";
import { useCreateAgentStep } from "@/pages/setup/add-capability-wizard/steps/create/use-create-agent-step";
import { ReviewAgentStep } from "@/pages/setup/add-capability-wizard/steps/review/review-agent-step";
import { CAPABILITY_OPTIONS } from "@/pages/setup/add-capability-wizard/steps/capability/capability-options";
import { Badge } from "@/components/shadcn/ui/badge";
import { getOptionLabel } from "@/lib/form-utils";

export const CAPABILITY_FLOW: AgentStepId[] = ["capability", "connection", "create", "review"];

export const useCapabilitySteps = (): WizardSteps<AgentStepId, AgentFormData> => {
    const connectProviderBeforeNext = useConnectProviderStep();
    const createAgentBeforeNext = useCreateAgentStep();

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
                return <Badge variant="secondary">{getOptionLabel(CAPABILITY_OPTIONS, values.capability.type)}</Badge>;
            },
        },
        connection: {
            title: "Connect your agent to AI Provider",
            description: "Choose the AI provider connection string your agent will use.",
            bodyComponent: ConnectProviderStep,
            validate: "connection",
            beforeNext: connectProviderBeforeNext,
            badgeFields: ["connection.connectionStringName"],
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
        },
        review: {
            title: "Review & edit your agent",
            bodyComponent: ReviewAgentStep,
            validate: "review",
        },
    };
};
