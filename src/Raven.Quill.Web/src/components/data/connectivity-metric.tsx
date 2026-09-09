import type { ConnectivityStatus } from "@/api/generated/server-api";
import { Text } from "@/components/typography";
import { cn } from "@/lib/utils";

// Healthy means the licensing API answered OK with no transport error.
function isConnectivityHealthy(connectivity: ConnectivityStatus) {
    return connectivity.statusCode === "OK" && !connectivity.exception;
}

export function ConnectivityMetric({ connectivity }: { connectivity: ConnectivityStatus }) {
    return (
        <div className="space-y-2">
            <div className="space-y-1">
                <Text variant="caption" as="div">
                    Connectivity
                </Text>
                <Text variant="label" as="div" className="flex items-center gap-2">
                    <span
                        className={cn(
                            "size-2 rounded-full",
                            isConnectivityHealthy(connectivity) ? "bg-emerald-500" : "bg-red-500",
                        )}
                        aria-hidden="true"
                    />
                    {connectivity.statusCode}
                </Text>
            </div>
            {connectivity.exception && (
                <Text variant="muted" className="break-words">
                    {connectivity.exception}
                </Text>
            )}
        </div>
    );
}
