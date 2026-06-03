import { useEffect } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { FormProvider, useForm } from "react-hook-form";
import { useNavigate, useParams } from "react-router";
import { useMutation } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { AiAgentConfiguration } from "@/api/generated/server-api";
import { appRoutes } from "@/lib/app-routes";
import { FormWizard } from "@/components/form/wizard/form-wizard";
import { agentSchema, type AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { CAPABILITY_FLOW, useCapabilitySteps } from "@/pages/setup/add-capability-wizard/capability-wizard-flow";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";
import { preventEnterKeySubmission } from "@/lib/form-utils";

export function AddCapabilityWizard() {
    const { slug = "" } = useParams();
    const navigate = useNavigate();
    const resetStore = useCapabilityWizardStore((state) => state.reset);

    const form = useForm<AgentFormData>({
        mode: "onChange",
        resolver: zodResolver(agentSchema),
        defaultValues: getDefaultValues(),
    });

    useEffect(() => {
        resetStore();
        return resetStore;
    }, [resetStore]);

    const provisionMutation = useMutation({
        mutationFn: async (values: AgentFormData) => {
            const base = useCapabilityWizardStore.getState().suggestions[values.create.selectedIndex];

            if (!base) {
                throw new Error("No agent suggestion selected.");
            }

            const config: AiAgentConfiguration = {
                ...base,
                name: values.review.name.trim(),
                systemPrompt: values.create.systemPrompt.trim(),
                connectionStringName: values.connection.connectionStringName,
                // Actions / sub-agents aren't supported by the provision endpoint in this preview.
                actions: [],
                subAgents: [],
                disabled: false,
            };

            await api.services.apps.provisionAgent(slug, config);
            return config.name;
        },
        onSuccess: (name) => {
            toast.success(`Agent "${name}" created`);
            navigate(appRoutes.app(slug));
        },
        onError: (error) => {
            // Backend rejections can carry a full stack trace; show only the first line.
            const message = error instanceof Error ? error.message.split("\n")[0] : "Could not create agent.";
            toast.error(message);
        },
    });

    return (
        <FormProvider {...form}>
            <form
                onSubmit={form.handleSubmit(async (values) => {
                    // Errors surface via the mutation's onError toast; swallow so the rejected
                    // promise doesn't bubble out of handleSubmit.
                    await provisionMutation.mutateAsync(values).catch(() => {});
                })}
                onKeyDown={preventEnterKeySubmission}
                className="h-full"
            >
                <AddCapabilityWizardBody />
            </form>
        </FormProvider>
    );
}

function AddCapabilityWizardBody() {
    const { slug = "" } = useParams();
    const navigate = useNavigate();
    const steps = useCapabilitySteps();

    return (
        <FormWizard
            steps={steps}
            flow={CAPABILITY_FLOW}
            cancel={() => navigate(appRoutes.app(slug))}
            submitLabel="Save agent"
        />
    );
}

function getDefaultValues(): AgentFormData {
    return {
        capability: { type: "agent" },
        connection: { connectionStringName: "" },
        create: { selectedIndex: 0, systemPrompt: "" },
        review: { name: "" },
    };
}
