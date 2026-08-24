import { useState, type ReactNode } from "react";
import { Text } from "@/components/typography";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Sparkles } from "lucide-react";
import { api } from "@/api/api";
import { AI_LICENSE_UNAVAILABLE_MESSAGE } from "@/api/custom-services/assistant-service";
import type { AiHelperStatus } from "@/api/generated/server-api";
import { invalidateConsentBlockedSuggestions } from "@/lib/query-invalidation";
import { useAssistantConsent } from "@/components/layout/use-assistant-consent";
import { Alert, AlertDescription } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Checkbox } from "@/components/shadcn/ui/checkbox";
import {
    Dialog,
    DialogClose,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from "@/components/shadcn/ui/dialog";
import { Spinner } from "@/components/shadcn/ui/spinner";

const AI_TERMS_OF_USE_URL = "https://ravendb.net/legal/ravendb/ai-assistant-terms-of-use";

export function AssistantConsentGate() {
    const consentQuery = useAssistantConsent();

    if (consentQuery.isPending) {
        return (
            <AssistantConsentFrame>
                <Text variant="muted" className="flex items-center gap-2">
                    <Spinner />
                    Checking consent…
                </Text>
            </AssistantConsentFrame>
        );
    }

    if (consentQuery.isError) {
        return (
            <AssistantConsentFrame>
                <Alert variant="destructive" className="text-left">
                    <AlertDescription>
                        Could not check whether the AI assistant is available.{" "}
                        <Button variant="link" className="h-auto p-0" onClick={() => consentQuery.refetch()}>
                            Try again
                        </Button>
                    </AlertDescription>
                </Alert>
            </AssistantConsentFrame>
        );
    }

    if (consentQuery.data.status === "ConsentRequired") {
        return (
            <AssistantConsentFrame>
                <Text variant="muted">
                    The AI assistant sends your questions to the RavenDB AI service. It stays unavailable until you
                    review and accept the Terms of Use.
                </Text>
                <AssistantConsentDialog />
            </AssistantConsentFrame>
        );
    }

    return (
        <AssistantConsentFrame>
            <Alert variant="destructive" className="text-left">
                <AlertDescription>{AI_LICENSE_UNAVAILABLE_MESSAGE}</AlertDescription>
            </Alert>
        </AssistantConsentFrame>
    );
}

function AssistantConsentFrame({ children }: { children: ReactNode }) {
    return (
        <div className="flex flex-1 flex-col items-center justify-center gap-3 p-4 text-center">
            <div className="flex size-12 items-center justify-center rounded-full bg-muted">
                <Sparkles className="size-5 text-primary" aria-hidden="true" />
            </div>
            {children}
        </div>
    );
}

function AssistantConsentDialog() {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <Dialog open={isOpen} onOpenChange={setIsOpen}>
            <DialogTrigger asChild>
                <Button>Review the Terms of Use</Button>
            </DialogTrigger>
            <DialogContent className="sm:max-w-lg">
                {/* The acceptance lives in the body, remounted on every open, so a dialog that was
                    dismissed reopens with the box unchecked. */}
                <AssistantConsentDialogBody onGranted={() => setIsOpen(false)} />
            </DialogContent>
        </Dialog>
    );
}

function describeConsentFailure(status: AiHelperStatus) {
    return status === "ConsentRequired"
        ? "The AI service has not registered the consent yet. Please try again in a moment."
        : AI_LICENSE_UNAVAILABLE_MESSAGE;
}

function AssistantConsentDialogBody({ onGranted }: { onGranted: () => void }) {
    const [isAccepted, setIsAccepted] = useState(false);
    const queryClient = useQueryClient();

    const consentMutation = useMutation({
        mutationFn: async () => {
            const result = await api.services.assistant.giveConsent();
            if (result.status !== "Success") {
                throw new Error(describeConsentFailure(result.status));
            }
            return result;
        },
        onSuccess: (result) => {
            queryClient.setQueryData(api.queries.assistant.consent().queryKey, result);
            void invalidateConsentBlockedSuggestions(queryClient);
            onGranted();
        },
    });

    return (
        <>
            <DialogHeader>
                <DialogTitle>Get started with the AI assistant</DialogTitle>
                <DialogDescription>
                    The assistant answers questions about RavenDB and Quill. Your messages are sent to the RavenDB AI
                    service, so it is available only once you accept its Terms of Use.
                </DialogDescription>
            </DialogHeader>
            <label className="flex items-start gap-2 text-sm">
                <Checkbox
                    checked={isAccepted}
                    onCheckedChange={(value) => setIsAccepted(value === true)}
                    className="mt-0.5"
                />
                <span>
                    I accept the{" "}
                    <a
                        href={AI_TERMS_OF_USE_URL}
                        target="_blank"
                        rel="noreferrer"
                        className="text-primary hover:underline"
                    >
                        RavenDB AI Assistant Terms of Use
                    </a>
                </span>
            </label>
            {consentMutation.isError && (
                <Alert variant="destructive">
                    <AlertDescription>
                        {consentMutation.error instanceof Error
                            ? consentMutation.error.message
                            : "Could not record your consent."}
                    </AlertDescription>
                </Alert>
            )}
            <DialogFooter>
                <DialogClose asChild>
                    <Button type="button" variant="outline">
                        Cancel
                    </Button>
                </DialogClose>
                <Button
                    type="button"
                    disabled={!isAccepted || consentMutation.isPending}
                    onClick={() => consentMutation.mutate()}
                >
                    {consentMutation.isPending && <Spinner />}
                    Agree &amp; enable
                </Button>
            </DialogFooter>
        </>
    );
}
