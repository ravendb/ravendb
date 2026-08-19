import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { useAssistantStore } from "@/components/layout/assistant-store";

export function useAssistantConsent() {
    const isOpen = useAssistantStore((state) => state.isOpen);
    return useQuery({ ...api.queries.assistant.consent(), enabled: isOpen });
}
