import { useFormContext, useWatch } from "react-hook-form";
import type { WizardConfirmNext } from "@/components/form/wizard/form-wizard";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { computeConnectKey } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";
import { selectHasSession } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";

export function usePlannerConnectionConfirm(): WizardConfirmNext {
    const { control } = useFormContext<AppFormData>();
    const connection = useWatch({ control, name: "externalConnection" });
    const hasSession = useSetupWizardStore(selectHasSession);
    const plannerConnectKey = useSetupWizardStore((state) => state.plannerConnectKey);

    return {
        isRequired: hasSession && computeConnectKey(connection) !== plannerConnectKey,
        title: "Cancel the AI planner?",
        description:
            "The plan was made for the previous connection. Continuing stops the planner and discards the plan, and you start over on the plan step. This cannot be undone.",
        confirmLabel: "Cancel the planner",
    };
}
