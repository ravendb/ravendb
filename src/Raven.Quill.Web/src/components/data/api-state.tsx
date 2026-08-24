import type { ReactNode } from "react";
import { Button } from "@/components/shadcn/ui/button";

type ApiStateProps = {
    errorTitle?: string;
    isError?: boolean;
    isLoading?: boolean;
    loadingLabel?: string;
    skeleton?: ReactNode;
    onRetry?: () => void;
    children: ReactNode;
};

export function ApiState({
    children,
    errorTitle = "Could not load data",
    isError,
    isLoading,
    loadingLabel = "Loading...",
    skeleton,
    onRetry,
}: ApiStateProps) {
    if (isLoading) {
        // No skeleton means the shape is genuinely unknowable ahead of the data, or the wait
        // itself is the message. Say it in words rather than promise a shape we cannot draw.
        if (!skeleton) {
            return (
                <p role="status" className="text-sm text-muted-foreground">
                    {loadingLabel}
                </p>
            );
        }

        // The skeleton is decoration, so the caller's label stays in the tree as the accessible
        // name for the wait instead of being dropped along with the visible text.
        return (
            <div role="status">
                <span className="sr-only">{loadingLabel}</span>
                <div aria-hidden="true">{skeleton}</div>
            </div>
        );
    }

    if (isError) {
        return (
            <div className="max-w-md space-y-3">
                <h2 className="text-sm font-semibold">{errorTitle}</h2>
                <p className="text-sm text-muted-foreground">Refresh the page or try again in a moment.</p>
                {onRetry && (
                    <Button type="button" variant="outline" size="sm" onClick={onRetry}>
                        Retry
                    </Button>
                )}
            </div>
        );
    }

    return children;
}
