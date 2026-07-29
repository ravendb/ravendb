import { CircleCheckIcon, ShieldCheckIcon } from "lucide-react";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { cn } from "@/lib/utils";
import { useVerifyCdcState } from "@/pages/setup/add-app-wizard/steps/verify/use-verify-cdc-step";

/**
 * Runs the CDC dry run that Next runs, so the operator can settle the selection before leaving the step.
 * Lives in the selection overlay: verifying is only meaningful once at least one table is selected.
 */
export function VerifySchemaButton({ disabled }: { disabled: boolean }) {
    const { isVerifying, isVerified, isRunning, verify } = useVerifyCdcState();

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
            {isVerifying ? "Verifying schema..." : isVerified ? "Schema verified" : "Verify schema"}
        </button>
    );
}
