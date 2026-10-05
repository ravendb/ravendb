import { CircleCheck } from "lucide-react";
import { useAiConsent } from "@/components/ai-consent/use-ai-consent";
import { Text } from "@/components/typography";

export function AiConsentAcceptedNote() {
    const consent = useAiConsent();

    if (!consent.isGranted) {
        return null;
    }

    return (
        <div className="flex min-w-0 items-center gap-2">
            <CircleCheck className="size-4 shrink-0 text-success" aria-hidden="true" />
            <Text variant="muted" as="span" className="truncate">
                RavenDB AI Assistant Terms of Use accepted
            </Text>
        </div>
    );
}
