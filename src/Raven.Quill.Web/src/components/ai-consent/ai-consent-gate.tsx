import { useState, type ReactNode } from "react";
import { Sparkles } from "lucide-react";
import { AiConsentTermsCheckbox } from "@/components/ai-consent/ai-consent-terms";
import { useAiConsent, useGrantAiConsent } from "@/components/ai-consent/use-ai-consent";
import { Alert, AlertDescription } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
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
import { Text } from "@/components/typography";
import { cn } from "@/lib/utils";

export type AiConsentCopy = {
    gateDescription: ReactNode;
    dialogTitle: ReactNode;
    dialogDescription: ReactNode;
};

/** "panel" fills a surface with nothing else to show; "banner" sits above options that stay on screen, disabled. */
export type AiConsentGateVariant = "panel" | "banner";

type AiConsentGateProps = {
    copy: AiConsentCopy;
    variant?: AiConsentGateVariant;
};

/** Stands in for an AI-backed surface until the AI service can actually answer it. */
export function AiConsentGate({ copy, variant = "panel" }: AiConsentGateProps) {
    const consent = useAiConsent();

    if (consent.isPending) {
        // A banner sits above options that stay readable, so it waits instead of flashing a spinner over them.
        return variant === "banner" ? null : (
            <AiConsentLayout variant={variant} icon={<Spinner />} message="Checking consent…" />
        );
    }

    if (consent.unavailableReason) {
        return (
            <AiConsentLayout
                variant={variant}
                tone="destructive"
                message={consent.unavailableReason}
                action={
                    consent.isRetryable && (
                        <Button variant="outline" onClick={consent.recheck}>
                            Try again
                        </Button>
                    )
                }
            />
        );
    }

    if (consent.isConsentRequired) {
        return (
            <AiConsentLayout
                variant={variant}
                message={copy.gateDescription}
                action={<AiConsentDialog copy={copy} />}
            />
        );
    }

    return null;
}

type AiConsentLayoutProps = {
    variant: AiConsentGateVariant;
    tone?: "default" | "destructive";
    icon?: ReactNode;
    message: ReactNode;
    action?: ReactNode;
};

function AiConsentLayout({ variant, tone = "default", icon, message, action }: AiConsentLayoutProps) {
    const isBanner = variant === "banner";
    const isError = tone === "destructive";

    return (
        <div
            // Only the failure is an alert; a Terms of Use nobody has been asked for yet is not one.
            role={isError ? "alert" : undefined}
            className={cn(
                "flex gap-3",
                isBanner
                    ? "items-center rounded-lg border bg-card p-3 text-left"
                    : "flex-1 flex-col items-center justify-center p-4 text-center",
            )}
        >
            <div
                className={cn(
                    "flex shrink-0 items-center justify-center rounded-full bg-muted",
                    isBanner ? "size-9" : "size-12",
                )}
            >
                {icon ?? (
                    <Sparkles
                        className={cn(isBanner ? "size-4" : "size-5", isError ? "text-destructive" : "text-primary")}
                        aria-hidden="true"
                    />
                )}
            </div>
            <Text variant="muted" className={cn(isBanner && "flex-1", isError && "text-destructive")}>
                {message}
            </Text>
            {action}
        </div>
    );
}

function AiConsentDialog({ copy }: { copy: AiConsentCopy }) {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <Dialog open={isOpen} onOpenChange={setIsOpen}>
            <DialogTrigger asChild>
                <Button>Review the Terms of Use</Button>
            </DialogTrigger>
            <DialogContent className="sm:max-w-lg">
                {/* Remounted on every open, so a dismissed dialog reopens with the box unchecked. */}
                <AiConsentDialogBody copy={copy} onGranted={() => setIsOpen(false)} />
            </DialogContent>
        </Dialog>
    );
}

function AiConsentDialogBody({ copy, onGranted }: { copy: AiConsentCopy; onGranted: () => void }) {
    const [isAccepted, setIsAccepted] = useState(false);
    const consentMutation = useGrantAiConsent({ onGranted });

    return (
        <>
            <DialogHeader>
                <DialogTitle>{copy.dialogTitle}</DialogTitle>
                <DialogDescription>{copy.dialogDescription}</DialogDescription>
            </DialogHeader>
            <AiConsentTermsCheckbox
                isAccepted={isAccepted}
                onAcceptedChange={setIsAccepted}
                disabled={consentMutation.isPending}
            />
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
