import type { ReactNode } from "react";
import { useWatch } from "react-hook-form";
import { describeAiConsentBlock, useAiConsent } from "@/components/ai-consent/use-ai-consent";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

export function useMapAiConsentBlock(): { isNextDisabled: boolean; nextDisabledReason?: ReactNode } {
    const source = useWatch<AppFormData, "map.source">({ name: "map.source" });
    const consent = useAiConsent();

    if (source !== "ai-suggested") {
        return { isNextDisabled: false };
    }

    const reason = describeAiConsentBlock(consent, "Choose Manual to map the schema yourself.");

    return reason ? { isNextDisabled: true, nextDisabledReason: reason } : { isNextDisabled: false };
}
