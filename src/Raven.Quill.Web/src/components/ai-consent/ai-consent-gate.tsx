import { useState, type ComponentProps, type ReactNode } from "react";
import { Sparkles } from "lucide-react";
import { AiConsentTermsCheckbox } from "@/components/ai-consent/ai-consent-terms";
import { useAiConsent, useGrantAiConsent, type AiConsentState } from "@/components/ai-consent/use-ai-consent";
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
import { Heading, Text } from "@/components/typography";
import { cn } from "@/lib/utils";

export type AiConsentCopy = {
    gateDescription: ReactNode;
    dialogTitle: ReactNode;
    dialogDescription: ReactNode;
};

export type AiConsentHeroCopy = {
    title: ReactNode;
    description: ReactNode;
    acceptLabel: ReactNode;
};

type AiConsentGateVariant = "panel" | "banner" | "hero";

type AiConsentGateHeroProps = {
    variant: Extract<AiConsentGateVariant, "hero">;
    copy: AiConsentHeroCopy;
    figure?: ReactNode;
    onGranted?: () => void;
};

type AiConsentGateProps =
    | { variant?: Exclude<AiConsentGateVariant, "hero">; copy: AiConsentCopy }
    | AiConsentGateHeroProps;

export function AiConsentGate(props: AiConsentGateProps) {
    if (props.variant === "hero") {
        return <AiConsentHero copy={props.copy} figure={props.figure} onGranted={props.onGranted} />;
    }

    return <AiConsentNotice copy={props.copy} variant={props.variant ?? "panel"} />;
}

function AiConsentNotice({ copy, variant }: { copy: AiConsentCopy; variant: AiConsentGateVariant }) {
    const consent = useAiConsent();

    if (consent.isPending) {
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
                action={<AiConsentRetryButton consent={consent} />}
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

function AiConsentHero({ copy, figure, onGranted }: Omit<AiConsentGateHeroProps, "variant">) {
    const consent = useAiConsent();
    const acceptance = useConsentAcceptance(onGranted);

    if (consent.isPending || consent.isGranted) {
        return null;
    }

    return (
        <div className="@container flex min-h-0 flex-1 overflow-auto px-5 py-10 sm:px-10 lg:px-14">
            <div className="m-auto grid w-full max-w-245 grid-cols-1 items-center gap-8 @3xl:grid-cols-[minmax(0,1fr)_minmax(0,1.15fr)] @3xl:gap-14">
                <div className="flex flex-col gap-7">
                    <div className="flex flex-col gap-3">
                        <Heading as="h3" variant="page" className="text-balance">
                            {copy.title}
                        </Heading>
                        <Text variant="muted" className="max-w-[44ch]">
                            {copy.description}
                        </Text>
                    </div>
                    <div className="flex flex-col gap-3.5">
                        {consent.unavailableReason ? (
                            <>
                                <Text role="alert" className="text-destructive">
                                    {consent.unavailableReason}
                                </Text>
                                <AiConsentRetryButton consent={consent} size="lg" className="self-start" />
                            </>
                        ) : (
                            <>
                                <AiConsentTermsCheckbox
                                    isAccepted={acceptance.isAccepted}
                                    onAcceptedChange={acceptance.setIsAccepted}
                                    disabled={acceptance.isPending}
                                />
                                <Button
                                    size="lg"
                                    className="self-start"
                                    disabled={!acceptance.isAccepted || acceptance.isPending}
                                    onClick={acceptance.accept}
                                >
                                    {acceptance.isPending ? <Spinner /> : <Sparkles aria-hidden="true" />}
                                    {copy.acceptLabel}
                                </Button>
                            </>
                        )}
                        <AiConsentGrantError error={acceptance.error} />
                    </div>
                </div>
                {figure}
            </div>
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
    const acceptance = useConsentAcceptance(onGranted);

    return (
        <>
            <DialogHeader>
                <DialogTitle>{copy.dialogTitle}</DialogTitle>
                <DialogDescription>{copy.dialogDescription}</DialogDescription>
            </DialogHeader>
            <AiConsentTermsCheckbox
                isAccepted={acceptance.isAccepted}
                onAcceptedChange={acceptance.setIsAccepted}
                disabled={acceptance.isPending}
            />
            <AiConsentGrantError error={acceptance.error} />
            <DialogFooter>
                <DialogClose asChild>
                    <Button type="button" variant="outline">
                        Cancel
                    </Button>
                </DialogClose>
                <Button
                    type="button"
                    disabled={!acceptance.isAccepted || acceptance.isPending}
                    onClick={acceptance.accept}
                >
                    {acceptance.isPending && <Spinner />}
                    Agree &amp; enable
                </Button>
            </DialogFooter>
        </>
    );
}

function useConsentAcceptance(onGranted?: () => void) {
    const [isAccepted, setIsAccepted] = useState(false);
    const consentMutation = useGrantAiConsent();

    return {
        isAccepted,
        setIsAccepted,
        isPending: consentMutation.isPending,
        error: consentMutation.error,
        // Given to mutate rather than to the mutation, so a prompt the operator already left does not act on the grant.
        accept: () => consentMutation.mutate(undefined, { onSuccess: onGranted }),
    };
}

function AiConsentGrantError({ error }: { error: Error | null }) {
    if (!error) {
        return null;
    }

    return (
        <Alert variant="destructive">
            <AlertDescription>{error.message}</AlertDescription>
        </Alert>
    );
}

function AiConsentRetryButton({
    consent,
    size,
    className,
}: {
    consent: AiConsentState;
    size?: ComponentProps<typeof Button>["size"];
    className?: string;
}) {
    if (!consent.isRetryable) {
        return null;
    }

    return (
        <Button variant="outline" size={size} className={className} onClick={consent.recheck}>
            Try again
        </Button>
    );
}
