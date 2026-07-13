import type { ReactNode } from "react";
import { Button } from "@/components/shadcn/ui/button";

type ApiStateProps = {
    errorTitle?: string;
    isError?: boolean;
    isLoading?: boolean;
    loadingLabel?: string;
    onRetry?: () => void;
    children: ReactNode;
};

export function ApiState({
    children,
    errorTitle = "Could not load data",
    isError,
    isLoading,
    loadingLabel = "Loading...",
    onRetry,
}: ApiStateProps) {
    if (isLoading) {
        return <p className="text-sm text-muted-foreground">{loadingLabel}</p>;
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
