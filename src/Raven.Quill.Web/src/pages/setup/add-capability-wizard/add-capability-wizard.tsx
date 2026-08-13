import { useEffect } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { FormProvider, useForm } from "react-hook-form";
import { useNavigate, useParams, useSearchParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { appRoutes } from "@/lib/app-routes";
import { FormWizard } from "@/components/form/wizard/form-wizard";
import { agentSchema, type AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { emptyAgentConfiguration } from "@/pages/setup/add-capability-wizard/agent-config-form";
import { CAPABILITY_FLOW, useCapabilitySteps } from "@/pages/setup/add-capability-wizard/capability-wizard-flow";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";
import { preventEnterKeySubmission } from "@/lib/form-utils";

export function AddCapabilityWizard() {
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

    return (
        <FormProvider {...form}>
            {/* The agent is provisioned in the review step's beforeNext (so the wizard can advance
                to the optional channels step), not on form submit. The form element just provides
                the RHF context and swallows stray Enter-key submits. */}
            <form onSubmit={(event) => event.preventDefault()} onKeyDown={preventEnterKeySubmission} className="h-full">
                <AddCapabilityWizardBody />
            </form>
        </FormProvider>
    );
}

function AddCapabilityWizardBody() {
    const { slug = "" } = useParams();
    const [searchParams] = useSearchParams();
    const navigate = useNavigate();
    const steps = useCapabilitySteps();
    const createdAgent = useCapabilityWizardStore((state) => state.createdAgent);

    // The channels step is optional, so the completion button reads "Skip for now" until the
    // operator actually connects a channel. Shares the channels-step query via React Query cache.
    const channelsQuery = useQuery({
        ...api.queries.channels.list(slug),
        enabled: Boolean(createdAgent),
    });
    const hasChannels =
        createdAgent != null && (channelsQuery.data ?? []).some((channel) => channel.agentId === createdAgent.agentId);

    // "Add agent" links here with ?capability=agent; the capability step is then already
    // answered (the form defaults to "agent"), so the wizard starts at the connection step.
    const isAgentPreselected = searchParams.get("capability") === "agent";

    return (
        <FormWizard
            steps={steps}
            flow={CAPABILITY_FLOW}
            initialStep={isAgentPreselected ? "connection" : undefined}
            cancel={() => navigate(appRoutes.app(slug))}
            // The agent is provisioned mid-flow, so from then on the draft is already persisted.
            isSaved={createdAgent !== null}
            completion={{
                type: "action",
                label: hasChannels ? "Finish" : "Skip for now",
                onComplete: () => navigate(appRoutes.app(slug)),
            }}
        />
    );
}

function getDefaultValues(): AgentFormData {
    return {
        capability: { type: "agent" },
        connection: { connectionStringName: "" },
        create: { mode: "ai", selectedIndex: 0, promptInput: "" },
        review: emptyAgentConfiguration(),
    };
}
