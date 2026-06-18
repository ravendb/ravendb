import { useEffect } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { FormProvider, useForm } from "react-hook-form";
import { useNavigate, useParams, useSearchParams } from "react-router";
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

    // "Add agent" links here with ?capability=agent; the capability step is then already
    // answered (the form defaults to "agent"), so the wizard starts at the connection step.
    const isAgentPreselected = searchParams.get("capability") === "agent";

    return (
        <FormWizard
            steps={steps}
            flow={CAPABILITY_FLOW}
            initialStep={isAgentPreselected ? "connection" : undefined}
            cancel={() => navigate(appRoutes.app(slug))}
            completion={{
                type: "action",
                label: "Finish",
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
