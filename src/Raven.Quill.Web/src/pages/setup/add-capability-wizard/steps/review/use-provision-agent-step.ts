import { useFormContext } from "react-hook-form";
import { useParams } from "react-router";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { AiAgentConfiguration } from "@/api/generated/server-api";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import type { WizardProgress } from "@/components/form/wizard/form-wizard";
import {
    buildActionBindings,
    buildAgentConfigurationPayload,
} from "@/pages/setup/add-capability-wizard/agent-config-form";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";
import { invalidateAgentQueries } from "@/lib/query-invalidation";

// Runs on "Save agent" from the review step: provisions the agent, then lets the wizard
// advance to the optional channels step. The new agent id is stored so that step can attach
// channels to it. Success shows a toast; failure throws so the wizard surfaces it inline via
// its advance error.
export function useProvisionAgentStep() {
    const { slug = "" } = useParams();
    const { getValues } = useFormContext<AgentFormData>();
    const queryClient = useQueryClient();
    const setCreatedAgent = useCapabilityWizardStore((state) => state.setCreatedAgent);

    return async (progress: WizardProgress) => {
        // Provisioning is create-only. The channels step has no Back, so this guards only the
        // defensive case — never create a second agent for the same wizard run.
        if (useCapabilityWizardStore.getState().createdAgent) {
            return;
        }

        const values = getValues();
        const base = resolveAgentBase(values);
        const config: AiAgentConfiguration = {
            ...base,
            ...buildAgentConfigurationPayload(values),
        };
        const name = values.review.name.trim();

        progress.report("Creating agent...");

        let agentId: string;
        try {
            const result = await api.services.apps.provisionAgent(slug, {
                configuration: config,
                actionBindings: buildActionBindings(values),
            });
            agentId = result.agentId;
        } catch (error) {
            // Backend rejections can carry a full stack trace; surface only the first line.
            const message = error instanceof Error ? error.message.split("\n")[0] : "Could not create agent.";
            throw new Error(message, { cause: error });
        }

        // Record the irreversible write before non-critical cache work. If invalidation ever
        // fails, the wizard must still know not to provision the same agent again.
        setCreatedAgent({ agentId, name });
        void invalidateAgentQueries(queryClient, slug);
        toast.success(`Agent "${name}" created`);
    };
}

// The AI candidate a provisioned agent builds on: the selected data suggestion ("ai"), the
// prompt-generated config ("prompt"), or none ("manual"). Throws when the expected candidate
// is missing so we never silently provision an empty agent.
function resolveAgentBase(values: AgentFormData): AiAgentConfiguration | undefined {
    const store = useCapabilityWizardStore.getState();

    if (values.create.mode === "ai") {
        const base = store.suggestions[values.create.selectedIndex];
        if (!base) {
            throw new Error("No agent suggestion selected.");
        }
        return base;
    }

    if (values.create.mode === "prompt") {
        const base = store.promptResult?.config;
        if (!base) {
            throw new Error("No generated agent. Go back and generate one from your prompt.");
        }
        return base;
    }

    return undefined;
}
