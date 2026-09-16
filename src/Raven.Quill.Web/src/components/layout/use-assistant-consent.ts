import { useAiConsentQuery } from "@/components/ai-consent/use-ai-consent";
import { useAssistantStore } from "@/components/layout/assistant-store";

export function useAssistantConsent() {
    const isOpen = useAssistantStore((state) => state.isOpen);
    return useAiConsentQuery({ enabled: isOpen });
}
