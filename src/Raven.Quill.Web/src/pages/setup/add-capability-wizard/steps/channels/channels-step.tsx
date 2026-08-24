import { useParams } from "react-router";
import { Alert } from "@/components/shadcn/ui/alert";
import { ChannelsSection } from "@/pages/apps/channels-section";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";

// The wizard's final, optional step. Reached only after the agent is provisioned; lets the
// operator attach channels to the new agent or finish and do it later.
export function ChannelsStep() {
    const { slug = "" } = useParams();
    const createdAgent = useCapabilityWizardStore((state) => state.createdAgent);

    // The flow only advances here after provisioning succeeds, so createdAgent is set; guard
    // defensively in case the step is rendered out of order.
    if (!createdAgent) {
        return <Alert>Create the agent first to add channels.</Alert>;
    }

    return <ChannelsSection slug={slug} agent={createdAgent} nested />;
}
