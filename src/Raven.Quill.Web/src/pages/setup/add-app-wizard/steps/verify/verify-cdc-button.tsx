import { CircleAlertIcon, CircleCheckIcon, ShieldCheckIcon } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { cn } from "@/lib/utils";
import type { VerifyCdcState } from "@/pages/setup/add-app-wizard/steps/verify/use-verify-cdc-step";

type VerifyCdcButtonProps = {
    disabled: boolean;
    /** Passed in rather than derived here: the step needs the same state for its error alert, and one
     * table selection should be watched once. */
    state: VerifyCdcState;
    labels: { idle: string; verifying: string; verified: string };
    /** "text" blends in with surrounding inline actions, "outline" stands alone as a regular button. */
    variant?: "text" | "outline";
};

/** Runs the CDC dry run that Next runs for the step's table set, so the operator can settle the
 * inputs before leaving the step. */
export function VerifyCdcButton({ disabled, state, labels, variant = "text" }: VerifyCdcButtonProps) {
    const { isVerifying, isVerified, isRunning, error, verify } = state;
    const hasError = Boolean(error) && !isVerifying && !isVerified;

    const icon = isVerifying ? (
        <Spinner className="size-3.5" />
    ) : isVerified ? (
        <CircleCheckIcon className="size-3.5" aria-hidden="true" />
    ) : hasError ? (
        <CircleAlertIcon className="size-3.5" aria-hidden="true" />
    ) : (
        <ShieldCheckIcon className="size-3.5" aria-hidden="true" />
    );
    const label = isVerifying ? labels.verifying : isVerified ? labels.verified : labels.idle;
    const isDisabled = disabled || isVerified || isRunning;

    if (variant === "outline") {
        return (
            <Button
                variant="outline"
                size="sm"
                onClick={verify}
                disabled={isDisabled}
                className={cn(
                    isVerified && "text-success disabled:opacity-100",
                    hasError && "text-destructive hover:text-destructive",
                )}
            >
                {icon}
                {label}
            </Button>
        );
    }

    return (
        <button
            type="button"
            onClick={verify}
            disabled={isDisabled}
            className={cn(
                "flex items-center gap-1.5 whitespace-nowrap transition-colors disabled:pointer-events-none",
                isVerified
                    ? "text-success"
                    : hasError
                      ? "text-destructive hover:text-destructive/80"
                      : "text-foreground hover:text-muted-foreground",
            )}
        >
            {icon}
            {label}
        </button>
    );
}
