import type { ReactNode } from "react";
import { TriangleAlert } from "lucide-react";
import type { BootstrapPhase } from "@/api/generated/server-api";
import { AuthScreenLayout } from "@/components/auth/auth-screen-layout";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { useActivationPolling } from "@/pages/auth/use-activation-polling";

// Gates the whole app on appliance startup. While the appliance activates, admin
// calls 503 and the auth status is unknowable, so we poll bootstrap status and show
// a "starting" screen until it reports Ready — then render the app (which routes to
// /login when unauthenticated).
export function BootGate({ children }: { children: ReactNode }) {
    const { phase, isReady, timedOut, retry } = useActivationPolling();

    if (isReady) {
        return children;
    }

    return (
        <AuthScreenLayout>
            <section className="w-full rounded-xl border bg-card p-6 text-center shadow-sm">
                {timedOut ? <BootTimedOut onRetry={retry} /> : <BootStarting phase={phase} />}
            </section>
        </AuthScreenLayout>
    );
}

function BootStarting({ phase }: { phase?: BootstrapPhase }) {
    return (
        <>
            <Spinner className="mx-auto size-7 text-primary-strong" />
            <h1 className="mt-4 text-lg font-semibold tracking-tight">Starting Quill</h1>
            <p className="mt-1.5 text-sm text-muted-foreground">{getStartingMessage(phase)}</p>
        </>
    );
}

function BootTimedOut({ onRetry }: { onRetry: () => void }) {
    return (
        <>
            <div className="mx-auto flex size-11 items-center justify-center rounded-full bg-destructive/10 text-destructive">
                <TriangleAlert className="size-5" aria-hidden="true" />
            </div>
            <h1 className="mt-4 text-lg font-semibold tracking-tight">Still starting…</h1>
            <p className="mt-1.5 text-sm text-muted-foreground">
                Quill is taking longer than usual to come online. Keep waiting, or try again.
            </p>
            <Button className="mt-5 w-full" onClick={onRetry}>
                Try again
            </Button>
        </>
    );
}

function getStartingMessage(phase?: BootstrapPhase) {
    switch (phase) {
        case "Redeeming":
            return "Activating your license…";
        case "Restarting":
            return "Applying configuration and restarting…";
        case "NeedsActivation":
            return "Preparing Quill…";
        default:
            return "This can take up to a minute on first run.";
    }
}
