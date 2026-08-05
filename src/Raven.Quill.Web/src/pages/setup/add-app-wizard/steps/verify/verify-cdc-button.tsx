import { CircleCheckIcon, ShieldCheckIcon } from "lucide-react";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { cn } from "@/lib/utils";
import type { VerifyCdcState } from "@/pages/setup/add-app-wizard/steps/verify/use-verify-cdc-step";

type VerifyCdcButtonProps = {
    disabled: boolean;
    /** Passed in rather than derived here: the step needs the same state for its error alert, and one
     * table selection should be watched once. */
    state: VerifyCdcState;
    labels: { idle: string; verifying: string; verified: string };
};

/** Runs the CDC dry run that Next runs for the step's table set, so the operator can settle the
 * inputs before leaving the step. */
export function VerifyCdcButton({ disabled, state, labels }: VerifyCdcButtonProps) {
    const { isVerifying, isVerified, isRunning, verify } = state;

    return (
        <button
            type="button"
            onClick={verify}
            disabled={disabled || isVerified || isRunning}
            className={cn(
                "flex items-center gap-1.5 whitespace-nowrap transition-colors disabled:pointer-events-none",
                isVerified ? "text-success" : "text-foreground hover:text-muted-foreground",
            )}
        >
            {isVerifying ? (
                <Spinner className="size-3.5" />
            ) : isVerified ? (
                <CircleCheckIcon className="size-3.5" aria-hidden="true" />
            ) : (
                <ShieldCheckIcon className="size-3.5" aria-hidden="true" />
            )}
            {isVerifying ? labels.verifying : isVerified ? labels.verified : labels.idle}
        </button>
    );
}
